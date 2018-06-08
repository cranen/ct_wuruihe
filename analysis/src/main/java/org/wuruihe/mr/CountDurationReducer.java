package org.wuruihe.mr;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.wuruihe.key.CommDimension;
import org.wuruihe.value.CountDurationValue;

import java.io.IOException;

/**
 * 根据map的每一种key,统计两人打电话的总次数，以及总时间
 */
public class CountDurationReducer extends Reducer<CommDimension,Text,CommDimension,CountDurationValue> {

    private  CountDurationValue v=new CountDurationValue();
    int countsum;//次数
    int durationSum;//打电话的总时长
    @Override
    protected void reduce(CommDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        countsum=0;//通话总次数
        durationSum=0;//通话总时长
        //循环累加
        for (Text value : values) {
            countsum++;
            durationSum+=Integer.valueOf(value.toString());
        }
        v.setCountSum(countsum+"");
        v.setDurationSum(durationSum+"");
        //写出去
        context.write(key,v);
    }
}
