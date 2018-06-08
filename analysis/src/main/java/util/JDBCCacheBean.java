package util;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 自己思考是否需要手动加锁
 */
public class JDBCCacheBean {

    private static Connection connection;

    private JDBCCacheBean() {
    }

    /**
     * 懒汉式双重锁定
     * @return
     */
    public static Connection getInstance() {
        try {
            if (connection == null || connection.isClosed()) {
                //connection = JDBCUtil.getConnection();
                synchronized (JDBCCacheBean.class){
                    if(connection==null|| connection.isClosed()){
                        connection=JDBCUtil.getConnection();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
