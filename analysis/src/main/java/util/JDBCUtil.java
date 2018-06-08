package util;

import java.sql.*;

public class JDBCUtil {
    /**
     *   private static final Logger logger = LoggerFactory.getLogger(JDBCUtil.class);
     private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
     private static final String MYSQL_URL = "jdbc:mysql://linux01:3306/db_telecom?useUnicode=true&characterEncoding=UTF-8";
     private static final String MYSQL_USERNAME = "root";
     private static final String MYSQL_PASSWORD = "123456";
     */
    private static final String MYSQL_JDBC_DRIVER="com.mysql.jdbc.Driver";
    private static final String MYSQL_JDBC_URL="jdbc:mysql://hadoop101:3306/ct_wuruihe?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_JDBC_USERNAME="root";
    private static final String MYSQL_JDBC_PASSWORD="123456";

    public static Connection getConnection()
    {
        try {
            Class.forName(MYSQL_JDBC_DRIVER);
            return DriverManager.getConnection(MYSQL_JDBC_URL,MYSQL_JDBC_USERNAME,MYSQL_JDBC_PASSWORD);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet){
        if(resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        System.out.println( getConnection());

    }
}
