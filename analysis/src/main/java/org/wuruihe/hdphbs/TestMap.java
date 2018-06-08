package org.wuruihe.hdphbs;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.security.Key;

/**
 * 拿一个表中列祖下的一个列
 */
public class TestMap extends TableMapper<ImmutableBytesWritable,Put>{
   //

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put=new Put(key.get());
        for (Cell cell:value.rawCells() ) {
            String cf= Bytes.toString(CellUtil.cloneFamily(cell));
            if(cf.equals("info")){
                String cn=Bytes.toString(CellUtil.cloneQualifier(cell));
                if(cn.equals("name")){
                    //Bytes.toString(CellUtil.cloneValue(cell));
                    put.add(cell);
                }
            }

        }
        context.write(key,put);
        //super.map(key, value, context);
    }
}
