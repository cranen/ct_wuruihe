package org.wuruihe.convertor;

import com.mysql.jdbc.util.LRUCache;
import org.wuruihe.key.BaseDimension;
import org.wuruihe.key.ContactDimension;
import org.wuruihe.key.DateDimension;
import util.JDBCCacheBean;
import util.JDBCUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DimensionConvertorimpl implements IConvertor {
    //缓存的最大值（遇到问题：lruCache的值是否时条数？？？）
    LRUCache lruCache = new LRUCache(3000);

    public int getDimensionID(BaseDimension baseDimension) {

        //1、查询缓存是否有值
        String cacheKey = getCacheKey(baseDimension);

        if (lruCache.containsKey(cacheKey)) {

            return Integer.parseInt(lruCache.get(cacheKey).toString());
        }

        //2、获取MySQL中是否有值。如果没有，插入数据
        String[] sqls = getSqls(baseDimension);
        Connection connection = JDBCCacheBean.getInstance();
        //3、 执行sql
        int id = -1;
        try {
            id = execSql(sqls, connection, baseDimension);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if(id==-1)throw new RuntimeException("未匹配到相应维度");
        //4、获取mysql中的值
        //5将数据放入缓存
        lruCache.put(cacheKey,id);
        //6返回id

        return id;
    }

    /**
     * 执行数据
     * @param sqls
     * @param connection
     * @param baseDimension
     * @return
     * @throws SQLException
     */
    private int execSql(String[] sqls, Connection connection, BaseDimension baseDimension) throws SQLException {

        int id=-1;
        PreparedStatement ps=null;

        try {
            ps= connection.prepareStatement(sqls[0]);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        //第一次查询
        setArguments(ps,baseDimension);
        ResultSet resultSet = ps.executeQuery();
        if(resultSet.next()){
            return resultSet.getInt(1);
        }

        //查询不到，插入数据
        ps=connection.prepareStatement(sqls[1]);
        setArguments(ps,baseDimension);
        ps.executeUpdate();

        //第二此查询
        ps=connection.prepareStatement(sqls[0]);
        setArguments(ps,baseDimension);
        resultSet = ps.executeQuery();
        if(resultSet.next()){
            return  resultSet.getInt(1);
        }

        return id;

    }

    /**
     *给执行参数赋值
     * @param ps
     * @param baseDimension
     * @throws SQLException
     */
    private void setArguments(PreparedStatement ps, BaseDimension baseDimension) throws SQLException {
        int i=0;
        if (baseDimension instanceof ContactDimension){
            ContactDimension contactDimension= (ContactDimension)baseDimension;
            ps.setString(++i,contactDimension.getPhoneNum());
            ps.setString(++i,contactDimension.getName());
        }else {
            DateDimension dateDimension=(DateDimension)baseDimension;
            ps.setInt(++i, Integer.parseInt(dateDimension.getYear()));
            ps.setInt(++i, Integer.parseInt(dateDimension.getDay()));
            ps.setInt(++i, Integer.parseInt(dateDimension.getMonth()));
        }


    }

    private String getCacheKey(BaseDimension baseDimension) {
        //StringBuffer是线程安全的，StringBuild线程不安全
        StringBuffer sb = new StringBuffer();
        if (baseDimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimension;

                sb.append(lruCache.get(contactDimension.getPhoneNum()));
        } else if (baseDimension instanceof DateDimension) {
            DateDimension dataDimension = (DateDimension) baseDimension;
           // String yearMonthDay = dataDimension.getYear() + dataDimension.getMonth() + dataDimension.getDay();
            // sb.append(dataDimension.getYear())

           sb.append(dataDimension.getYear()).append(dataDimension.getMonth()).append(dataDimension.getDay());
        }
        return sb.toString();
    }

    /**
     * 获取getSqls中是否有值
     * @param baseDimension
     * @return
     */
    private String[] getSqls(BaseDimension baseDimension) {
        String[] sqls = new String[2];
        if (baseDimension instanceof ContactDimension) {
            sqls[0] = "SELECT id FROM tb_contacts WHERE telephone=? and `name`=?";
            sqls[1] = "INSERT INTO tb_contacts   VALUES(NULL,?,?);";

        } else if (baseDimension instanceof DateDimension) {
            sqls[0] = "SELECT id FROM tb_dimension_date WHERE `year`=? AND `month`=? AND `day`=?";
            sqls[1] = "INSERT INTO tb_dimension_date VALUES (NULL,?,?,?)";
        }
        return sqls;
    }

}
