package org.wuruihe.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wuruihe.convertor.DimensionConvertorimpl;
import org.wuruihe.key.BaseDimension;
import org.wuruihe.key.CommDimension;
import org.wuruihe.value.CountDurationValue;
import util.JDBCCacheBean;
import util.JDBCUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutFormat extends OutputFormat<BaseDimension, CountDurationValue> {
    private FileOutputCommitter committer = null;

    public RecordWriter<BaseDimension, CountDurationValue> getRecordWriter(TaskAttemptContext context) {
        //获取单例连接
        Connection connection = JDBCCacheBean.getInstance();
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new MysqlRecordWriter(connection);
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    private static Path getOutPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

//静态内部类(真正输出内容的)

    /**
     * 从reduce中读出来的数据为
     * call1  2012_-1_-1
     */
    static class MysqlRecordWriter extends RecordWriter<BaseDimension, CountDurationValue> {
        private Connection connection = null;//连接
        private PreparedStatement preparedStatement = null;//预编译器
        private int batchBound = 500;//缓存sql条数边界
        private int batchsize = 0;//客户端已经缓存的条数

        public MysqlRecordWriter(Connection connection) {
            this.connection = connection;
        }

        public void write(BaseDimension key, CountDurationValue value) throws IOException, InterruptedException {
            CommDimension commDimension = (CommDimension) key;

            //插入数据
            String sql = "INSERT INTO tb_call VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE `call_sum`=?,`call_duration_sum`=?";
            //维度转换
            DimensionConvertorimpl cOnvertorimpl = new DimensionConvertorimpl();

            //表中

            int dateId = cOnvertorimpl.getDimensionID(commDimension.getDateDimension());
            int contactid = cOnvertorimpl.getDimensionID(commDimension.getContactDimension());
            int countSum = Integer.valueOf(value.getCountSum());
            int durationSum = Integer.valueOf(value.getDurationSum());
            String date_contact = dateId + "+" + contactid;


            //拼接值
            int i = 0;
            try {
                if(preparedStatement==null) {
                    preparedStatement = connection.prepareStatement(sql);
                }
                preparedStatement.setString(++i, date_contact);
                preparedStatement.setInt(++i, dateId);
                preparedStatement.setInt(++i, contactid);
                preparedStatement.setInt(++i, countSum);
                preparedStatement.setInt(++i, durationSum);
                preparedStatement.setInt(++i, countSum);
                preparedStatement.setInt(++i, durationSum);
                //将数据缓存到客户端
                preparedStatement.addBatch();
                batchsize++;
                if (batchsize >= batchBound) {
                    //批量执行sql
                    preparedStatement.executeUpdate();
                    connection.commit();
                    //batchSize归零处理
                    batchsize=0;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            //此方法用于关闭连接
            try {
                if (preparedStatement != null) {

                    preparedStatement.executeBatch();
                    connection.commit();
                }
               // JDBCUtil.close(connection, null, null);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
