package org.wuruihe.coprocesspr;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectInstance;
import utils.HbaseDaoUtil;
import utils.PropertiesUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * 协处理器（企业处理器）
 *
 * 查看类中所有的方法的快捷键（alter+7）
 *
 * 将其hbase的bin目录下放入consumerjar包，并分发
 *
 * hbase配置文件配置上企业管理器，详情见文档
 */
public class CalleeWriteObsercer extends BaseRegionObserver {
    //获取regions的个数
    private int regions = Integer.valueOf(PropertiesUtils.properties.getProperty("hbase.regions"));
    //格式化
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        /**
         * 00_15961260091_2019-03-05 10:04:05_13157770954_1_0673
         0x_13157770954_2019-03-05 10:04:05_15961260091_0_0673
         */

        //System.out.println("1");
        String buildtime_ts = null;
        //  super.postPut(e, put, edit, durability);
        //获取当前操作的表
        String tablename = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        String curTablename = PropertiesUtils.properties.getProperty("hbase.table.name");
        if (!(tablename.equals(curTablename))) return;

        //获取之前数据的rowkey
        String row = Bytes.toString(put.getRow());
        String[] split = row.split("_");
        String flag = split[4];

        if ("0".equals(flag)) return;

        String caller = split[1];
        String buildTime = split[2];
        String callee = split[3];
        String duration = split[5];

        try {
            buildtime_ts = sdf.parse(buildTime).getTime() + "";
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
        //获取分区
        String regionHash = HbaseDaoUtil.getRegionHash(callee, buildTime, regions);
        String rowKey = HbaseDaoUtil.getRowKey(regionHash, callee, buildTime, caller, "0",duration);
        Put newput = new Put(Bytes.toBytes(rowKey));
        newput.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(caller));
        newput.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildtime"), Bytes.toBytes(buildTime));
        newput.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildtime_ts"), Bytes.toBytes(buildtime_ts));
        newput.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(callee));
        newput.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
        newput.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //System.out.println("2");
        //获取连接
      //  Connection connection = ConnectInstance.getInstance();
      //  Table table = connection.getTable(TableName.valueOf(tablename));


       // table.put(newput);e
         Table table=  e.getEnvironment().getTable(TableName.valueOf(tablename));
         table.put(newput);
         table.close();
        //System.out.println("3");
    }
}
