package org.apache.cassandra.hadoop.cql3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.AbstractColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.cql3.ColumnFamilyRecordReader;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class ColumnFamilyInputFormat extends AbstractColumnFamilyInputFormat<List<IColumn>, Map<ByteBuffer, IColumn>>
{

    @Override
    public RecordReader<List<IColumn>, Map<ByteBuffer, IColumn>> getRecordReader(InputSplit split, JobConf jobConf, final Reporter reporter)
            throws IOException
    {
        TaskAttemptContext tac = new TaskAttemptContext(jobConf, TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID)))
        {
            @Override
            public void progress()
            {
                reporter.progress();
            }
        };

        ColumnFamilyRecordReader recordReader = new ColumnFamilyRecordReader(jobConf.getInt(CASSANDRA_HADOOP_MAX_KEY_SIZE, CASSANDRA_HADOOP_MAX_KEY_SIZE_DEFAULT));
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit)split, tac);
        return recordReader;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<List<IColumn>, Map<ByteBuffer, IColumn>> createRecordReader(
            org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException
    {
        return new ColumnFamilyRecordReader();
    }

}
