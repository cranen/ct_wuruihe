package utils;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.wuruihe.Constants.Constant;

import java.io.IOException;

/**
 * 单列
 * 用来描述连接
 */

public class ConnectInstance {
    private static Connection conn;
    private ConnectInstance (){}
    public static Connection getInstance() {
        if (conn == null || conn.isClosed()) {
            try {
                //将配置文件加入连接中
                conn = ConnectionFactory.createConnection(Constant.CONF);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return conn ;
    }

}
