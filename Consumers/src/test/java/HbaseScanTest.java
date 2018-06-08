import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectInstance;

import java.io.IOException;
import java.text.ParseException;

public class HbaseScanTest {
    public static void main(String[] args) throws ParseException, IOException {
//        //14314302040,2019-01,2019-04
//        String phone = "14314302040";
//        String startPoint = "2019-01";
//        String endPoint = "2019-05";2019-05

        HbaseScanUtil scanUtil=new HbaseScanUtil();
        scanUtil.init("14314302040","2019-01","2019-05");
        Connection connection = ConnectInstance.getInstance();
        Table table = connection.getTable(TableName.valueOf("ct_wrh:calllog"));
        while (scanUtil.hasNext()){
            String[] rowkeys = scanUtil.next();
            Scan scan=new Scan ();
            scan.setStartRow(Bytes.toBytes(rowkeys[0]));
            scan.setStopRow(Bytes.toBytes(rowkeys[1]));
            System.out.println("时间范围："+rowkeys[0].split("_")[2]
            +"----------------"
            +rowkeys[1].split("_")[2]);
            ResultScanner scanner=table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
            }
        }
    }
}
