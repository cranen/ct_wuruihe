package org.wuruihe;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectInstance;
import utils.HbaseFilterUtil;

import java.io.IOException;

public class HbaseFileterTest {

    public static void main(String[] args) throws IOException {



        Connection connection = ConnectInstance.getInstance();
        Table table = connection.getTable(TableName.valueOf("ct_wh:calllog"));
        Scan scan=new Scan();
        //电话号码过滤
        Filter filter1 = HbaseFilterUtil.eqFilter("f1", "call1", Bytes.toBytes("17519874292"));
        Filter filter2 = HbaseFilterUtil.eqFilter("f1", "call2", Bytes.toBytes("17519874292"));

        Filter filter3 = HbaseFilterUtil.orFilter(filter1, filter2);


        Filter filter4 = HbaseFilterUtil.gteqFilter("f1", "buildtime", Bytes.toBytes("2019-3"));
        Filter filter5 = HbaseFilterUtil.gteqFilter("f1", "buildtime", Bytes.toBytes("2019-6"));

        Filter filter6 = HbaseFilterUtil.andFilter(filter4, filter5);
        Filter filter = HbaseFilterUtil.andFilter(filter3, filter6);

        //scan.getFilter();
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print (Bytes.toString(CellUtil.cloneRow(cell))+",");
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cell))+",");
                System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell))+", ");
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell))+"   ");

            }
           // System.out.println();

        }





    }
}
