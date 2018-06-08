package utils;

import com.sun.tools.doclint.Checker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;


public class HbaseDaoUtil {


    private static Configuration conf = HBaseConfiguration.create();

    /**
     * 判断表是否存在
     *
     * @param tablename
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tablename) throws IOException {
      //  Connection connection = ConnectionFactory.createConnection(conf);
        Connection connection = ConnectInstance.getInstance();
        Admin admin = connection.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf(tablename));
        close(connection,admin);
        return result ;
    }

    /**
     * 创建命名空间
     *
     * @param namespace
     * @throws IOException
     */
    public static void initNamespace(String namespace) throws IOException {
       // Scan
      //  Filter
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor create_ts = NamespaceDescriptor.create(namespace).addConfiguration("create_ts", String.valueOf(System.currentTimeMillis())).build();
       // NamespaceDescriptor create_ts = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(create_ts);//没有对外暴露判断命名孔家是否存在的方法
        //处理方法，try--catch :在一查内打印表已经存在
        close(connection, admin);
    }

    /**
     * 创建表
     * @param tablename
     * @param columnfamily
     * @throws IOException
     */
    public static void createTable(String tablename, String... columnfamily) throws IOException {
        if (isTableExist(tablename)) {
            System.out.println("表已经存在");
            return;
        }
        //连接hbase
        Connection connection = ConnectInstance.getInstance();
        //获取hbase的管理者
        Admin admin = connection.getAdmin();
        //获取表的描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));

        //创建的列族
        for (String s : columnfamily) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        //添加协处理器
        hTableDescriptor.addCoprocessor("org.wuruihe.coprocesspr.CalleeWriteObsercer");

        //创建的分区
        int region = Integer.valueOf(PropertiesUtils.properties.getProperty("hbase.regions"));
        admin.createTable(hTableDescriptor, getSplitKeys(region));
        close(connection,admin);
    }


    /**
     * 预分区
     * 将一个table分成多个region.
     * 针对同一张表，一个regionserver维护两个region即可。
     *
     * @return
     */
    public static byte[][] getSplitKeys(int region) {
        DecimalFormat df = new DecimalFormat("00");
        byte[][] splitkeys = new byte[region][];
        for (int i=0;i<region;i++){
            //toBytes()
            splitkeys[i] = Bytes.toBytes(df.format(i)+ "|");
        }
        for (byte[] splitkey : splitkeys) {
            System.out.println(Bytes.toString(splitkey));
        }
        return splitkeys;
    }

    /**
     * 生成rowkey（分区号+数据）
     * @param regionHash
     * @param caller
     * @param buildTimer
     * @param callee
     * @param duration
     * @return
     * "_"+flag+
     */
    public static String getRowKey(String regionHash, String caller, String buildTimer, String callee,String flag, String duration) {
        return regionHash + "_" + caller + "_" + buildTimer + "_" + callee + "_"+ flag + "_" + duration;
    }

    /**
     *此方法将每条数据分发到不同的分区
     * @param caller
     * @param buildTime：获取时间的年和月，追加为rowkey
     * @param regions
     * @return
     */
    public static String getRegionHash(String caller,String buildTime ,int regions){
        int len =caller.length();
        //获取手机号后四位
        String last4Num=caller.substring(len -4);
        //获取年月
        String yearMoth = buildTime.replaceAll("-", "").substring(0, 6);
       // System.out.println("分区中没有截取前时间的格式"+buildTime);
        int regioncode = (Integer.valueOf(last4Num) ^ Integer.valueOf(yearMoth)) % regions;
        DecimalFormat df=new DecimalFormat("00");
        return df.format(regioncode);
    }
    /**

     * @param tables  * 关闭资源
     *
     * @param connection
     * @param admin
     */
    public static void close(Connection connection, Admin admin, Table... tables) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (tables.length <= 0)
            return;
        for (Table table : tables) {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
//        getSplitKeys(6);
//        String regionHash = getRegionHash("17868457605", "2019-02-21", 6);
//        String rowKey = getRowKey(regionHash, "17868457605", "2019-02-21", "15064972307", "1234");
//        System.out.println(rowKey);
        getSplitKeys(6);

        System.out.println(getRegionHash("15961260092", "2019-02-21 13:13:13", 6));

        System.out.println(getRowKey(getRegionHash("15961260092", "2019-02-21 13:13:13", 6), "15961260092", "2019-02-21 13:13:13", "17130206814", "","0180"));


    }


}
