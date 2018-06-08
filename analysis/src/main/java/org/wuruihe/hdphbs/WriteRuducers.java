package org.wuruihe.hdphbs;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * 一个rowkey对应一个put对象
 * 但也可以将一个cell封装到put中
 */
public class WriteRuducers extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
      for(Put put:values){

          context.write(NullWritable.get(),put);

      }

    }
}
