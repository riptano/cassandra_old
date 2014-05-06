/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop.cql3;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.ClientHolder;
import org.apache.cassandra.hadoop.cql3.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
/**
 * Hadoop RecordReader read the values return from the CQL query
 * It use CQL key range query to page through the wide rows.
 * 
 * Return List<IColumn> as keys columns
 * 
 * Map<ByteBuffer, IColumn> as column name to columns mappings
 */
public class ColumnFamilyRecordReader extends RecordReader<List<IColumn>, Map<ByteBuffer, IColumn>>
    implements org.apache.hadoop.mapred.RecordReader<List<IColumn>, Map<ByteBuffer, IColumn>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyRecordReader.class);

    public static final int CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT = 8192;
    public static final int DEFAULT_CQL_PAGE_LIMIT = 1000; // TODO: find the number large enough but not OOM

    private ColumnFamilySplit split;
    private RowIterator iter;
    
    private Pair<List<IColumn>, Map<ByteBuffer, IColumn>> currentRow;
    private int totalRowCount; // total number of rows to fetch
    private String keyspace;
    private String cfName;
    private ClientHolder client;
    private ConsistencyLevel consistencyLevel;
    private int keyBufferSize = 8192;
    
    // partition keys -- key aliases
    private List<Key> partitionKeys = new ArrayList<Key>();
    
    // cluster keys -- column aliases
    private List<Key> clusterKeys = new ArrayList<Key>();
    
    // map prepared query type to item id
    private Map<Integer, Integer> preparedQueryIds = new HashMap<Integer, Integer>();
    
    // cql query select columns
    private String columns;
    
    // the number of cql rows per page
    private int pageRowSize;
    
    // user defined where clauses
    private String userDefinedWhereClauses;
    
    private IPartitioner partitioner;

    public ColumnFamilyRecordReader()
    {
        this(ColumnFamilyRecordReader.CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT);
    }
    
    public ColumnFamilyRecordReader(int keyBufferSize)
    {
        super();
        this.keyBufferSize = keyBufferSize;
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = context.getConfiguration();
        totalRowCount = (this.split.getLength() < Long.MAX_VALUE)
                ? (int) this.split.getLength()
                : ConfigHelper.getInputSplitSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getReadConsistencyLevel(conf));
        keyspace = ConfigHelper.getInputKeyspace(conf);
        columns = CQLConfigHelper.getInputcolumns(conf);
        userDefinedWhereClauses = CQLConfigHelper.getInputWhereClauses(conf);
        
        try
        {
            pageRowSize = Integer.parseInt(CQLConfigHelper.getInputPageRowSize(conf));
        }
        catch (NumberFormatException e)
        {
            pageRowSize = DEFAULT_CQL_PAGE_LIMIT;         
        }

        partitioner = ConfigHelper.getInputPartitioner(context.getConfiguration());
        
        try
        {
            // only need to connect once
            if (client != null && client.transport.isOpen())
                return;

            // create connection using thrift
            String location = getLocation();

            int port = ConfigHelper.getInputRpcPort(conf);
            client = ColumnFamilyInputFormat.createAuthenticatedClient(location, port, conf);
            
            // retrieve partition keys and cluster keys from system.schema_columnfamilies table
            retrieveKeys();

            client.thriftClient.set_keyspace(keyspace);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        
        iter = new RowIterator();

        logger.debug("created {}", iter);
    }

    public void close()
    {
        if (client != null)
            client.close();
    }

    @Override
    public List<IColumn> getCurrentKey()
    {
        return currentRow.left;
    }

    @Override
    public Map<ByteBuffer, IColumn> getCurrentValue()
    {
        return currentRow.right;
    }

    @Override
    public float getProgress()
    {
        if (!iter.hasNext())
            return 1.0F;

        // the progress is likely to be reported slightly off the actual but close enough
        float progress = ((float) iter.totalRead / totalRowCount);
        return progress > 1.0F ? 1.0F : progress;
    }

    @Override
    public boolean nextKeyValue() throws IOException
    {
        if (!iter.hasNext())
        {
            logger.debug("Finished scanning " + iter.totalRead + " rows (estimate was: " + totalRowCount + ")");
            return false;
        }

        currentRow = iter.next();
        return true;
    }

    // we don't use endpointsnitch since we are trying to support hadoop nodes that are
    // not necessarily on Cassandra machines, too.  This should be adequate for single-DC clusters, at least.
    private String getLocation()
    {
        Collection<InetAddress> localAddresses = FBUtilities.getAllLocalAddresses();

        for (InetAddress address : localAddresses)
        {
            for (String location : split.getLocations())
            {
                InetAddress locationAddress = null;
                try
                {
                    locationAddress = InetAddress.getByName(location);
                }
                catch (UnknownHostException e)
                {
                    throw new AssertionError(e);
                }
                if (address.equals(locationAddress))
                {
                    return location;
                }
            }
        }
        return split.getLocations()[0];
    }

    // Because the old Hadoop API wants us to write to the key and value
    // and the new asks for them, we need to copy the output of the new API
    // to the old. Thus, expect a small performance hit.
    // And obviously this wouldn't work for wide rows. But since ColumnFamilyInputFormat
    // and ColumnFamilyRecordReader don't support them, it should be fine for now.
    @Override
    public boolean next(List<IColumn> keys, Map<ByteBuffer, IColumn> value) throws IOException
    {
        if (this.nextKeyValue())
        {
            keys = getCurrentKey();

            value.clear();
            value.putAll(getCurrentValue());

            return true;
        }
        return false;
    }

    @Override
    public long getPos() throws IOException
    {
        return (long) iter.totalRead;
    }

    @Override
    public List<IColumn> createKey()
    {
        return new ArrayList<IColumn>();
    }

    @Override
    public Map<ByteBuffer, IColumn> createValue()
    {
        return new HashMap<ByteBuffer, IColumn>();
    }

    /** CQL row iterator */
    private class RowIterator extends AbstractIterator<Pair<List<IColumn>, Map<ByteBuffer, IColumn>>>
    {
        protected int totalRead = 0;             // total number of cf rows read
        protected Iterator<CqlRow> iterator;
        private int pageRows = 0;                // the number of cql rows read of this page
        private String previousRowKey = null;    // previous CF row key
        private String partitionKeyString;       // keys in <key1>, <key2>, <key3> string format
        private String partitionKeyQuestions;    // question marks in ? , ? , ? format which matches the number of keys
        
        public RowIterator()
        {
            // initial page
            iterator = executeQuery();
        }
        
        @Override
        protected Pair<List<IColumn>, Map<ByteBuffer, IColumn>> computeNext()
        {
            if (iterator == null)
                return endOfData();
            
            int index = -2;
            //check there are more page to read
            while (!iterator.hasNext())
            {
                // no more data
                if (index == -1)
                {
                    logger.debug("no more data.");
                    return endOfData();
                }
                    
                // set last non-null key value to null
                logger.debug("set tail to null");
                index = setTailNull(clusterKeys);
                iterator = executeQuery();
                pageRows = 0;
                
                if (iterator == null || !iterator.hasNext() && index < 0)
                {
                    logger.debug("no more data.");
                    return endOfData();
                }
            }
            
            Map<ByteBuffer, IColumn> valueColumns = createValue();
            List<IColumn> keyColumns = new ArrayList<IColumn>();
            int i = 0;
            CqlRow row = iterator.next();
            for (Column column: row.columns)
            {
                String columnName = getString(column.getName());
                logger.debug("column: " + columnName);
                
                if (i<partitionKeys.size() + clusterKeys.size())
                    keyColumns.add(unthriftify(column));
                else
                    valueColumns.put(column.name, unthriftify(column));   

                i++;
            }
            
            // increase total CQL row read for this page
            pageRows++;
            
            // increase total CF row read
            if(newRow(keyColumns, previousRowKey))
                totalRead++;
                        
            // read full page
            if (pageRows >= pageRowSize || !iterator.hasNext())
            {
                // update partition keys
                Iterator<IColumn> newKeys = keyColumns.iterator();
                Iterator<Key> keys = partitionKeys.iterator();
                while(keys.hasNext())
                {
                    keys.next().value = newKeys.next().value();
                }               

                // update cluster keys
                keys = clusterKeys.iterator();
                while(keys.hasNext())
                {                
                    Key key = keys.next();
                    key.value = newKeys.next().value();
                }
                
                iterator = executeQuery();
                pageRows = 0;
                
                if (iterator == null)
                    return endOfData();
            }

            return Pair.create(keyColumns, valueColumns);
        }

        /** convert thrift column to Cassandra column */
        private IColumn unthriftify(Column column)
        {
            return new org.apache.cassandra.db.Column(column.name, column.value, column.timestamp);
        }
        
        /** check whether start to read a new CF row by comparing the partition keys */
        private boolean newRow(List<IColumn> keyColumns, String previousRowKey)
        {
            if (keyColumns.size() == 0)
                return false;
            
            String rowKey = "";
            if (keyColumns.size() == 1)
                rowKey = partitionKeys.get(0).validator.getString(
                                                 ByteBufferUtil.clone(
                                                     keyColumns.get(0).value()));
            else
            {
                Iterator<Key> keys = partitionKeys.iterator();
                Iterator<IColumn> itera = keyColumns.iterator();
                while (keys.hasNext())
                {
                    rowKey = rowKey + keys.next().validator.getString(
                                                     ByteBufferUtil.clone(
                                                             itera.next().value())) + ":";
                }
            }
            
            logger.debug("previous RowKey: " + previousRowKey + ", new row key: " + rowKey);
            
            if (previousRowKey == null)
            {
                this.previousRowKey = rowKey;
                return true;
            }
            
            if (rowKey.equals(previousRowKey))
                return false;
           
            this.previousRowKey = rowKey;
            return true;
        }
        
        /** set the last non-null key value to null, and return the previous index */
        private int setTailNull(List<Key> values)
        {
            if (values.size() == 0)
                return 0;
            
            Iterator<Key> iterator = values.iterator();
            int previousIndex = -1;
            Key current = null;
            while (iterator.hasNext())
            {
                current = iterator.next();
                if (current.value == null)
                {
                    int index = previousIndex > 0 ? previousIndex : 0;
                    Key key = values.get(index);
                    logger.debug("set key " + key.name + " value to  null");
                    key.value = null;
                    return previousIndex - 1 ;
                }

                previousIndex++;
            }
                        
            Key key = values.get(previousIndex);
            logger.debug("set key " + key.name + " value to null");
            key.value = null;
            return previousIndex - 1;
        }

        /** compose the prepared query, pair.left is query id, pair.right is query */
        private Pair<Integer, String> composeQuery(String columns)
        {
            Pair<Integer, String> clause = whereClause();
            if (columns == null)
                columns = "*";
            else
            {
                // add keys in the front in order
                String partitionKey = keyString(partitionKeys);               
                String clusterKey = keyString(clusterKeys);
                if ("".equals(clusterKey))
                    columns = partitionKey + "," + columns;
                else
                    columns = partitionKey + "," + clusterKey + "," + columns;
            }
            
            return Pair.create(clause.left,
                               "SELECT " + columns 
                               + " FROM " + cfName 
                               +  clause.right
                               + (userDefinedWhereClauses == null ? "" : " AND " + userDefinedWhereClauses)
                               + " LIMIT " + pageRowSize
                               + " ALLOW FILTERING");
        }
        
        /** compose the where clause */
        private Pair<Integer, String> whereClause()
        {
            if (partitionKeyString == null)
                partitionKeyString = keyString(partitionKeys);
            
            if (partitionKeyQuestions == null)
                partitionKeyQuestions = partitionKeyQuestions();
            //initial query token(k) >= start_token and token(k) <= end_token
            if (emptyValues(partitionKeys))
                return Pair.create(0,
                                   " WHERE token(" + partitionKeyString + ") >= ? AND token(" + partitionKeyString + ") <= ?");
            else
            {
                //query token(k) > token(pre_partition_key) and token(k) <= end_token
                if (clusterKeys.get(0).value == null)
                    return Pair.create(1, 
                                       " WHERE token(" + partitionKeyString + ") > token(" + partitionKeyQuestions + ") "
                                       + " AND token(" + partitionKeyString + ") <= ?");
                else
                {
                    //query token(k) = token(pre_partition_key) and m = pre_cluster_key_m and n > pre_cluster_key_n 
                    Pair<Integer, String> clause = whereClause(clusterKeys, 0);
                    return Pair.create(clause.left, 
                                       " WHERE token(" + partitionKeyString + ") = token(" + partitionKeyQuestions + ") "
                                       + clause.right);
                }
            }
        }
        
        /** recursively compose the where clause */
        private Pair<Integer, String> whereClause(List<Key> keys, int position)
        {
            if (position == keys.size() -1 || keys.get(position + 1).value == null)
                return Pair.create(position + 2, " AND " + keys.get(position).name + " > ? " );
            else
            {
                Pair<Integer, String> clause = whereClause(keys, position + 1);
                return Pair.create(clause.left, 
                                   " AND " + keys.get(position).name + " = ? " 
                                   + clause.right);
            }
        }
        
        /** check whether all key values are null */
        private boolean emptyValues(List<Key> keys)
        {
            if (keys.size() == 0)
                return true;
            
            for (Key key : keys)
            {
                if (key.value != null)
                    return false;
            }
            return true;
        }
        
        /** compose the partition key string in format of <key1>, <key2>, <key3> */
        private String keyString(List<Key> keys)
        {
            String result = null;
            boolean first = true;
            for (Key key : keys)
            {
                if (first)
                    result =  key.name;
                else
                    result = result + "," + key.name;
                first = false;
            }
            
            return result;
        }
        
        /** compose the question marks for partition key string in format of ?, ? , ? */
        private String partitionKeyQuestions()
        {
            String result = null;
            boolean first = true;
            for (Key key : partitionKeys)
            {
                if (first)
                    result =  "?";
                else
                    result = result + ",?";
                
                first = false;
            }
            
            return result;
        }
        
        /** compose the query binding variables, pair.left is query id, pair.right is the binding variables */
        private Pair<Integer, List<ByteBuffer>> preparedQueryBindValues()
        {
            List<ByteBuffer> values = new LinkedList<ByteBuffer>();
            
            //initial query token(k) >= start_token and token(k) <= end_token
            if (emptyValues(partitionKeys))
            {
                values.add(partitioner.getTokenValidator().fromString(split.getStartToken()));
                values.add(partitioner.getTokenValidator().fromString(split.getEndToken()));
                return Pair.create(0, values);
            }
            else
            {
                //partition key values
                Iterator<Key> partitionKey = partitionKeys.iterator();
                while (partitionKey.hasNext())
                    values.add(partitionKey.next().value);
                
                //query token(k) > token(pre_partition_key) and token(k) <= end_token
                if (clusterKeys.get(0).value == null)
                {
                    values.add(partitioner.getTokenValidator().fromString(split.getEndToken()));
                    return Pair.create(1, values);
                }
                else //query token(k) = token(pre_partition_key) and m = pre_cluster_key_m and n > pre_cluster_key_n 
                {
                    int type = preparedQueryBindValues(clusterKeys, 0, values);
                    return Pair.create(type, values);
                }
            }
        }
        
        /** recursively compose the query binding variables */
        private int preparedQueryBindValues(List<Key>keys, int position, List<ByteBuffer> bindValues)
        {
            if (position == keys.size() - 1 || keys.get(position + 1).value == null)
            {
                bindValues.add(keys.get(position).value);
                return position + 2;
            }
            else
            {
                bindValues.add(keys.get(position).value);
                return preparedQueryBindValues(keys, position + 1, bindValues);
            }
        }
        
        /**  get the prepared query item Id */
        private int prepareQuery(int type)
        {
            Integer itemId = preparedQueryIds.get(type);
            if (itemId != null)
                return itemId;
            
            Pair<Integer, String> query = null;
            try
            {
                query = composeQuery(columns);
                logger.debug("type:" + query.left + ", query: " + query.right);
                CqlPreparedResult cqlPreparedResult = 
                        client.thriftClient.prepare_cql3_query(ByteBufferUtil.bytes(query.right), Compression.NONE);
                preparedQueryIds.put(query.left, cqlPreparedResult.itemId);
                return cqlPreparedResult.itemId;
            }
            catch (InvalidRequestException e)
            {
                logger.error("failed to perpared query " + query.right, e);
            }
            catch (TException e)
            {
                logger.error("failed to perpared query " + query.right, e);
            }
            return -1;
        }
        
        /** execute the prepared query */
        private Iterator<CqlRow> executeQuery()
        {       
            Pair<Integer, List<ByteBuffer>> bindValues = preparedQueryBindValues();
            logger.debug("query type: " + bindValues.left);
            try
            {
                CqlResult cqlResult = client.thriftClient.execute_prepared_cql3_query(
                                                                       prepareQuery(bindValues.left), 
                                                                       bindValues.right, 
                                                                       consistencyLevel);
                if (cqlResult != null && cqlResult.rows != null)
                    return cqlResult.rows.iterator();
                else
                    return null;
            }
            catch (InvalidRequestException e)
            {
                logger.error("failed to execute perpared query", e);
            }
            catch (UnavailableException e)
            {
                logger.error("failed to execute perpared query", e);
            }
            catch (TimedOutException e)
            {
                logger.error("failed to execute perpared query", e);
            }
            catch (SchemaDisagreementException e)
            {
                logger.error("failed to execute perpared query", e);
            }
            catch (TException e)
            {
                logger.error("failed to execute perpared query", e);
            }
            return null;
        }
        
        /** get the string of the byte[] */
        private String getString(byte[] value)
        {
            try
            {
                return ByteBufferUtil.string(ByteBuffer.wrap(value));
            }
            catch (CharacterCodingException e)
            {
                logger.error("can't get the string from byte array", e);
            }
            return null;
        }
    }
    
    /** retrieve the partition keys and cluster keys from system.schema_columnfamilies table */
    private void retrieveKeys() throws Exception
    {
        String query = "select key_aliases," +
                "column_aliases, " +
                "key_validator, " +
                "comparator  " +
                "from system.schema_columnfamilies " +
                "where keyspace_name='%s' and columnfamily_name='%s'";
        String formatted = String.format(query, keyspace, cfName);
        CqlResult result = client.thriftClient.execute_cql3_query(
                              ByteBufferUtil.bytes(formatted),
                              Compression.NONE,
                              ConsistencyLevel.ONE);

        CqlRow cqlRow = result.rows.get(0);
        String keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(0).getValue()));
        logger.debug("partition keys: " + keyString);
        List<String> keys = FBUtilities.fromJsonList(keyString);

        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext())
        {
            partitionKeys.add(new Key(iterator.next()));
        }

        keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(1).getValue()));
        logger.debug("cluster keys: " + keyString);
        keys = FBUtilities.fromJsonList(keyString);

        iterator = keys.iterator();
        while (iterator.hasNext())
        {
            clusterKeys.add(new Key(iterator.next()));
        }
        
        Column rawKeyValidator = cqlRow.columns.get(2);
        String validator = ByteBufferUtil.string(ByteBuffer.wrap(rawKeyValidator.getValue()));
        logger.debug("row key validator: " + validator);
        AbstractType<?> keyValidator = parseType(validator);
        
        if (keyValidator instanceof CompositeType)
        {
            Iterator<AbstractType<?>> typeItera = ((CompositeType) keyValidator).types.iterator();
            Iterator<Key> keyItera = partitionKeys.iterator();
            while (typeItera.hasNext())
                keyItera.next().validator = typeItera.next();  
        }
        else
            partitionKeys.get(0).validator = keyValidator;

    }
    
    private AbstractType<?> parseType(String type) throws IOException
    {
        try
        {
            // always treat counters like longs, specifically CCT.compose is not what we need
            if (type != null && type.equals("org.apache.cassandra.db.marshal.CounterColumnType"))
                    return LongType.instance;
            return TypeParser.parse(type);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
        catch (SyntaxException e)
        {
            throw new IOException(e);
        }
    }
    
    private class Key
    {
        String name;
        ByteBuffer value;
        AbstractType<?> validator;
        
        public Key(String name)
        {
            this.name = name;
        }
    }
    
}
