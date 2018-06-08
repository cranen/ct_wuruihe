package dao;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectInstance;
import utils.HbaseDaoUtil;
import utils.PropertiesUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class HbaseDao {
    private SimpleDateFormat sdf;
    private String namespace;//命名空间
    private String tablename;//表明
    private Integer regions;
    private String cf;
    private String cf2;
    private HTable table;
    private String flag;//主被叫数据标志位
    //缓存put对象的集合
    private List<Put> listput;

    /**
     * hbase.regions=6
     * <p>
     * <p>
     * #hbase命名空间
     * hbase.namespace=ct_wuruihe
     * #hbase表明
     * #hbase.table.name=ct_wuruihe:calllog
     */

    public HbaseDao() throws IOException {
        //初始化相关属性（数据来源于配置文件kafka.properties）
        namespace = PropertiesUtils.properties.getProperty("hbase.namespace");
        tablename = PropertiesUtils.properties.getProperty("hbase.table.name");
        regions = Integer.valueOf(PropertiesUtils.properties.getProperty("hbase.regions"));
        cf = PropertiesUtils.properties.getProperty("hbase.table.cf");
        cf2=PropertiesUtils.properties.getProperty("hbase.table.cf2");
        sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        listput=new ArrayList<Put>();
        flag="1";//1代表主叫


        //初始化命名空间及表的创建
        if (!HbaseDaoUtil.isTableExist(tablename)) {
            HbaseDaoUtil.initNamespace(namespace);
            HbaseDaoUtil.createTable(tablename,cf,cf2);
        }
    }


    public void put(String ori) throws IOException, ParseException {
        if (ori == null) return;

        if(listput.size()==0){
            //获取连接
            Connection conn = ConnectInstance.getInstance();
            table=(HTable)conn.getTable(TableName.valueOf(tablename));
            //设置不能自动提交
            table.setAutoFlushTo(false);

            //设置客户端缓存大小
            table.setWriteBufferSize(1024*1024);

        }


        //ori:14314302040,19460860743,2019-05-08 23:41:05,0439
        String[] split = ori.split(",");
        //截取字段封装相关参数
        String caller = split[0];//主叫
        String callee = split[1];//被叫
        String buildTime = split[2];//通话建立时间
        String  time = sdf.parse(buildTime).getTime()+"";//通话建立时间戳
        String duration = split[3];//通话时长
        //获取分区
        String regionHash = HbaseDaoUtil.getRegionHash(caller, buildTime, regions);
        //获取rowkey
        String rowKey = HbaseDaoUtil.getRowKey(regionHash, caller, buildTime, callee,flag,duration);
        //向put中添加数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向put中添加数据（列族：列）（值）
        //call1,buildtime,buildtime_ts,call2,duration
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("call1"),Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("buildtime"),Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("buildtime_ts"),Bytes.toBytes(time));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call2"), Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        listput.add(put);
        if(listput.size()>20){
            table.put(listput);
            //手动提交
            table.flushCommits();
            //清空list集合
            listput.clear();
            //关闭连接
            table.close();
        }

    }


}
