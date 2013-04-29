package org.apache.cassandra.hadoop.cql3;

import org.apache.hadoop.conf.Configuration;

public class CQLConfigHelper
{
    private static final String INPUT_CQL_COLUMNS_CONFIG = "cassandra.input.columnfamily.columns"; // separate by colon ,
    private static final String INPUT_CQL_PAGE_ROW_SIZE_CONFIG = "cassandra.input.page.row.size";
    private static final String INPUT_CQL_WHERE_CLAUSE_CONFIG = "cassandra.input.where.clause";
    

    /**
     * Set the CQL columns for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param columns
     */
    public static void setInputColumns(Configuration conf, String columns)
    {
        if (columns == null || columns.isEmpty())
            return;
        
        conf.set(INPUT_CQL_COLUMNS_CONFIG, columns);
    }
    
    /**
     * Set the CQL 3 keys for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param cqlKeys
     */
    public static void setInputCQLPageRowSize(Configuration conf, String cqlPageRowSize)
    {
        if (cqlPageRowSize == null)
        {
            throw new UnsupportedOperationException("cql page row size may not be null");
        }

        conf.set(INPUT_CQL_PAGE_ROW_SIZE_CONFIG, cqlPageRowSize);
    }

    /**
     * Set the CQL user defined where clauses for the input of this job.
     *
     * @param conf Job configuration you are about to run
     * @param clauses
     */
    public static void setInputWhereClauses(Configuration conf, String clauses)
    {
        if (clauses == null || clauses.isEmpty())
            return;
        
        conf.set(INPUT_CQL_WHERE_CLAUSE_CONFIG, clauses);
    }
    
    
    public static String getInputcolumns(Configuration conf)
    {
        return conf.get(INPUT_CQL_COLUMNS_CONFIG);
    }
    
    public static String getInputPageRowSize(Configuration conf)
    {
        return conf.get(INPUT_CQL_PAGE_ROW_SIZE_CONFIG);
    }
    
    public static String getInputWhereClauses(Configuration conf)
    {
        return conf.get(INPUT_CQL_WHERE_CLAUSE_CONFIG);
    }
}
