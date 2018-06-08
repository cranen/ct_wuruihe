package org.wuruihe.value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 获取
 */
public class CountDurationValue extends BaseValue{
    //某个维度通话次数总和
    private String countSum;
    //某个维度通话时间总和
    private String durationSum;

    public void write(DataOutput out) throws IOException {
        out.writeUTF(countSum);
        out.writeUTF(durationSum);
    }

    public void readFields(DataInput in) throws IOException {
        this.countSum=in.readUTF();
        this.durationSum=in.readUTF();
    }

    /**
     * 无参数构造
     */
    public CountDurationValue() {}

    @Override
    public String toString() {
        return countSum + "\t" + durationSum;
    }


    public String getCountSum() {
        return countSum;
    }

    public String getDurationSum() {
        return durationSum;
    }

    public void setCountSum(String countSum) {
        this.countSum = countSum;
    }

    public void setDurationSum(String durationSum) {
        this.durationSum = durationSum;
    }



}
