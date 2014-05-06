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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.AbstractColumnFamilyRecordWriter;
import org.apache.cassandra.hadoop.ClientHolder;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.Progressable;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>ColumnFamilyRecordWriter</code> maps the output &lt;key, value&gt;
 * pairs to a Cassandra column family. In particular, it applies the binded variables
 * in the value to the prepared statement, which it associates with the key, and in 
 * turn the responsible endpoint.
 *
 * <p>
 * Furthermore, this writer groups the cql queries by the endpoint responsible for
 * the rows being affected. This allows the cql queries to be executed in parallel,
 * directly to a responsible endpoint.
 * </p>
 *
 * @see ColumnFamilyOutputFormat
 */
final class ColumnFamilyRecordWriter extends AbstractColumnFamilyRecordWriter<ByteBuffer, List<List<ByteBuffer>>>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyRecordWriter.class);
    
    // handles for clients for each range running in the threadpool
    private final Map<Range, RangeClient> clients;
    
    // host to prepared statement id mappings
    private ConcurrentHashMap<String, Integer> preparedStatements = new ConcurrentHashMap<String, Integer>();
    
    private final String preparedStatement;
    
    /**
     * Upon construction, obtain the map that this writer will use to collect
     * mutations, and the ring cache for the given keyspace.
     *
     * @param context the task attempt context
     * @throws IOException
     */
    ColumnFamilyRecordWriter(TaskAttemptContext context) throws IOException
    {
        this(context.getConfiguration());
        this.progressable = new Progressable(context);
    }

    ColumnFamilyRecordWriter(Configuration conf, Progressable progressable) throws IOException
    {
        this(conf);
        this.progressable = progressable;
    }

    ColumnFamilyRecordWriter(Configuration conf) throws IOException
    {
        super(conf);
        this.clients = new HashMap<Range, RangeClient>();
        preparedStatement = CQLConfigHelper.getOutputPreparedStatement(conf);
    }
    
    @Override
    public void close() throws IOException
    {
        // close all the clients before throwing anything
        IOException clientException = null;
        for (RangeClient client : clients.values())
        {
            try
            {
                client.close();
            }
            catch (IOException e)
            {
                clientException = e;
            }
        }
        if (clientException != null)
            throw clientException;
    }
    
    /**
     * If the key is to be associated with a valid value, a mutation is created
     * for it with the given column family and columns. In the event the value
     * in the column is missing (i.e., null), then it is marked for
     * {@link Deletion}. Similarly, if the entire value for a key is missing
     * (i.e., null), then the entire key is marked for {@link Deletion}.
     * </p>
     *
     * @param keybuff
     *            the key to write.
     * @param value
     *            the value to write.
     * @throws IOException
     */
    @Override
    public void write(ByteBuffer keybuff, List<List<ByteBuffer>> values) throws IOException
    {
        Range<Token> range = ringCache.getRange(keybuff);

        // get the client for the given range, or create a new one
        RangeClient client = clients.get(range);
        if (client == null)
        {
            // haven't seen keys for this range: create new client
            client = new RangeClient(ringCache.getEndpoint(range));
            client.start();
            clients.put(range, client);
        }

        for (List<ByteBuffer> bindValues : values)
            client.put(Pair.create(keybuff, bindValues));
            progressable.progress();
    }

    /**
     * A client that runs in a threadpool and connects to the list of endpoints for a particular
     * range. Binded variable values for keys in that range are sent to this client via a queue.
     */
    public class RangeClient extends AbstractRangeClient<List<ByteBuffer>>
    {
        /**
         * Constructs an {@link RangeClient} for the given endpoints.
         * @param endpoints the possible endpoints to execute the mutations on
         */
        public RangeClient(List<InetAddress> endpoints)
        {
            super(endpoints);
         }
        
        
        /**
         * Loops collecting cql binded variable values from the queue and sending to Cassandra
         */
        public void run()
        {
            outer:
            while (run || !queue.isEmpty())
            {
                Pair<ByteBuffer, List<ByteBuffer>> bindVariables;
                try
                {
                    bindVariables = queue.take();
                }
                catch (InterruptedException e)
                {
                    // re-check loop condition after interrupt
                    continue;
                }

                Iterator<InetAddress> iter = endpoints.iterator();
                while (true)
                {
                    // send the mutation to the last-used endpoint.  first time through, this will NPE harmlessly.
                    try
                    {
                        int i = 0;
                        int itemId = preparedStatement(client);
                        while (bindVariables != null)
                        {
                            client.thriftClient.execute_prepared_cql3_query(itemId, bindVariables.right, ConsistencyLevel.ONE);
                            i++;
                            
                            if (i >= batchThreshold)
                                break;
                            
                            bindVariables = queue.poll();
                        }
                        
                        break;
                    }
                    catch (Exception e)
                    {
                        closeInternal();
                        if (!iter.hasNext())
                        {
                            lastException = new IOException(e);
                            break outer;
                        }
                    }

                    // attempt to connect to a different endpoint
                    try
                    {
                        InetAddress address = iter.next();
                        String host = address.getHostName();
                        int port = ConfigHelper.getOutputRpcPort(conf);
                        client = ColumnFamilyOutputFormat.createAuthenticatedClient(host, port, conf);
                    }
                    catch (Exception e)
                    {
                        closeInternal();
                        // TException means something unexpected went wrong to that endpoint, so
                        // we should try again to another.  Other exceptions (auth or invalid request) are fatal.
                        if ((!(e instanceof TException)) || !iter.hasNext())
                        {
                            lastException = new IOException(e);
                            break outer;
                        }
                    }
                }
            }
        }

        /** get prepared statement id from cache, otherwise prepare it from Cassandra server*/
        private int preparedStatement(ClientHolder client)
        {
            
            Integer itemId = preparedStatements.get(client.host);
            if (itemId == null)
            {
                try
                {
                    CqlPreparedResult result = client.thriftClient.prepare_cql3_query(
                                                                   ByteBufferUtil.bytes(preparedStatement), 
                                                                   Compression.NONE);
                    Integer previousId = preparedStatements.putIfAbsent(client.host, Integer.valueOf(result.itemId));
                    if (previousId != null)
                        return previousId;
                    
                    return result.itemId;
                }
                catch (InvalidRequestException e)
                {
                    logger.error("failed to prepare cql query " + preparedStatement, e);
                }
                catch (TException e)
                {
                    logger.error("failed to prepare cql query " + preparedStatement, e);
                }
            }
            return itemId;
        }
    }
}
