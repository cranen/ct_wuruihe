import utils.HbaseDaoUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class HbaseScanUtil {
    private SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM");
    /**
     *  String phone = "14314302040";
        String startPoint = "2019-01";
        String endPoint = "2019-05";
     */
    private List<String[]>list;
    private int i=0;
    public void init(String phone,String start,String stop) throws ParseException {
        list=new ArrayList<String[]>();
        Date startDate = sdf.parse(start);//开始时间节点
        Date stopDate = sdf.parse(stop);//结束时间节点

        //当前开始时间
        Calendar startPoint = Calendar.getInstance();
        startPoint.setTime(startDate);
        //当前结束时间
        Calendar stopPoint = Calendar.getInstance();
        stopPoint.setTime(startDate);
        stopPoint.add(Calendar.MONTH,1);

        while(stopPoint.getTimeInMillis()<=stopDate.getTime()){

            String startTime = sdf.format(startPoint.getTime());
            String stopTime=sdf.format(stopPoint.getTime());

            //String buildtime = sdf.format(new Date(startPoint.getTimeInMillis()));
            String regionHash = HbaseDaoUtil.getRegionHash(phone, startTime, 6);
            //HbaseDaoUtil.getRowKey()
            String startRow=  regionHash+"_"+phone+"_"+startTime;
            String stopRow=  regionHash+"_"+phone+"_"+stopTime;

            String[]rowkeys={startRow,stopRow};
            list.add(rowkeys);

            startPoint.add(Calendar.MONTH,1);
            stopPoint.add(Calendar.MONTH,1);

        }



    }

    public boolean hasNext(){
        return i<list.size();
    }
    public String[] next(){
        return  list.get(i++);
    }
}
