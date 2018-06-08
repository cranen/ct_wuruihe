package org.wuruihe.hdphbs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WriteRunner implements Tool
{
    private Configuration conf=null;
    @Override
    public int run(String[] args) throws Exception {
        Job job=Job.getInstance(this.conf,getClass().getSimpleName());
        job.setJarByClass(WriteRunner.class);
        TableMapReduceUtil.initTableMapperJob("fruit",
                new Scan(),
                TestMap.class,
                ImmutableBytesWritable.class,
                Put.class,job);
        TableMapReduceUtil.initTableReducerJob("fruit_mr",WriteRuducers.class,job);


        return job.waitForCompletion(true)?0:1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf= HBaseConfiguration.create(conf);

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int status= ToolRunner.run(new WriteRunner(),args);
        System.exit(status);
    }
}
