package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HbaseDaoUtil {


    private static Configuration conf= HBaseConfiguration.create();
    public void initNamespace(String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();
        NamespaceDescriptor create_ts = NamespaceDescriptor.create(namespace).addConfiguration("create_ts", String.valueOf(System.currentTimeMillis())).build();
        admin.createNamespace(create_ts);
        close(connection,admin);

    }

    public static void createTable(String tablename,String...columnfamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf(tablename));
        for (String s : columnfamily) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        admin.createTable(hTableDescriptor);

    }






    public static byte[][] getSplitKeys(){
        return null;
    }
    /**
     * 关闭资源
     * @param connection
     * @param admin
     * @param tables
     */
    public  static  void close(Connection connection, Admin admin, Table...tables){
        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(admin!=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(tables.length<=0)
            return;
        for (Table table : tables) {
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
}}
