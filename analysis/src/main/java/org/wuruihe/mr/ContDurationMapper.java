package org.wuruihe.mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.wuruihe.key.CommDimension;
import org.wuruihe.key.ContactDimension;
import org.wuruihe.key.DateDimension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 维度表中于主叫和被叫没有关系
 *
 * 参数为什么不可以时抽象lei
 * 需要实例化对象
 *
 * 此map中key的可能值
 * call1_2012_-1_-1_call2(统计call1给call2打电话某一年,次数，总时长)
 * call1_2012_2_-1_call2（统计call1给call2打电话某一月,次数，总时长）
 * call1_2012_2-1_call2（统计call1给call2打电话某一天,次数，总时长）
 */
public class ContDurationMapper  extends TableMapper<CommDimension,Text>{

    Text v=new Text();

    Map<String, String> phoneName = new HashMap<String, String>();
    private void init(){

        phoneName.put("19251212343", "李雁");
        phoneName.put("15961260091", "卫艺");
        phoneName.put("17130206814", "仰莉");
        phoneName.put("18682499648", "陶欣悦");
        phoneName.put("15361960968", "施梅梅");
        phoneName.put("18356645821", "金虹霖");
        phoneName.put("17818674361", "魏明艳");
        phoneName.put("14266298447", "华贞");
        phoneName.put("13141904126", "华啟倩");
        phoneName.put("13157770954", "仲采绿");
        phoneName.put("19460860743", "卫丹");
        phoneName.put("14016550401", "戚丽红");
        phoneName.put("14314302040", "何翠柔");
        phoneName.put("17457157786", "钱溶艳");
        phoneName.put("15108090007", "钱琳");
        phoneName.put("15882276699", "缪静欣");
        phoneName.put("19694998088", "焦秋菊");
        phoneName.put("18264427294", "吕访琴");
        phoneName.put("17245432318", "沈丹");
        phoneName.put("16705495586", "褚美丽");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       // super.setup(context);
        init();//初始化方法
    }

    /**
     * 数据从hbase上写到hdfs 上的map方法中
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String rowkey = Bytes.toString(value.getRow());
        String[] split = rowkey.split("_");
        String flag=split[4];
        if("0".equals(flag))return;

        String call1=split[1];
        String call2=split[3];

        String buildtime = split[2];
        //切割获取时间
        String year = buildtime.substring(0, 4);
        String month = buildtime.substring(5, 7);
        String date=buildtime.substring(8,10);

        String duration=split[5];
        /**
         * 将数据导入到mysql中，统计一天中通话的此数，一月通话的次数，一年通话的次数
         * 一次这些数据要在mapreduce上统计以后存在MySQL上。
         * 实现此业务，分两个维度
         * 第一个维度为姓名与电话号码
         * 第二个维度为年月日
         * 即一年的数据表示为2013-(-1)-(-1)
         * 一个月的数据为2013-2-(-1)
         * 一天的数据2013-2-2
         */
        CommDimension commDimension = new CommDimension();
        ContactDimension contactDimension = new ContactDimension();
        DateDimension yearDimension=new DateDimension(year,"-1","-1");
        DateDimension monthDimension=new DateDimension(year,month,"-1");
        DateDimension dateDimension=new DateDimension(year,month,date);

        v.set(duration);
        //第一个联系人的封装
        contactDimension.setName(phoneName.get(call1));
        contactDimension.setPhoneNum(call1);
        //年维度
        commDimension.setContactDimension(contactDimension);
        commDimension.setDateDimension(yearDimension);

        context.write(commDimension,v);

        //月维度
        commDimension.setDateDimension(monthDimension);
        context.write(commDimension,v);
        //日维度
        commDimension.setDateDimension(dateDimension);
        context.write(commDimension,v);


        //第二个联系人维度封装
        contactDimension.setName(phoneName.get(call2));
        contactDimension.setPhoneNum(call2);

        commDimension.setDateDimension(yearDimension);
        context.write(commDimension,v);

        commDimension.setDateDimension(monthDimension);
        context.write(commDimension,v);

        commDimension.setDateDimension(dateDimension);
        context.write(commDimension,v);

    }
}
