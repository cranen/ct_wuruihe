package org.wuruihe;

import java.awt.*;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

/**
 * 模拟变化的log日志
 */
public class Producers {
    private String start = "2019-01-01";
    private String end = "2020-01-01";

    private ArrayList<String> phoneNum = new ArrayList<String>();

    private Map<String, String> phoneName = new HashMap<String, String>();

    private DecimalFormat df = new DecimalFormat("0000");
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 添加数据
     */
    public void init() {
        phoneNum.add("19251212343");
        phoneNum.add("15961260091");
        phoneNum.add("17130206814");
        phoneNum.add("18682499648");
        phoneNum.add("15361960968");
        phoneNum.add("18356645821");
        phoneNum.add("17818674361");
        phoneNum.add("14266298447");
        phoneNum.add("13141904126");
        phoneNum.add("13157770954");
        phoneNum.add("19460860743");
        phoneNum.add("14016550401");
        phoneNum.add("14314302040");
        phoneNum.add("17457157786");
        phoneNum.add("15108090007");
        phoneNum.add("15882276699");
        phoneNum.add("19694998088");
        phoneNum.add("18264427294");
        phoneNum.add("17245432318");
        phoneNum.add("16705495586");
        phoneNum.add("16705495586");

        phoneName.put("19251212343", "李雁");
        phoneName.put("15961260091", "卫艺");
        phoneName.put("17130206814", "仰莉");
        phoneName.put("18682499648", "陶欣悦");
        phoneName.put("15361960968", "施梅梅");
        phoneName.put("18356645821", "金虹霖");
        phoneName.put("17818674361", "魏明艳");
        phoneName.put("14266298447", "华贞");
        phoneName.put("13141904126", "华啟倩");
        phoneName.put("13157770954", "仲采绿");
        phoneName.put("19460860743", "卫丹");
        phoneName.put("14016550401", "戚丽红");
        phoneName.put("14314302040", "何翠柔");
        phoneName.put("17457157786", "钱溶艳");
        phoneName.put("15108090007", "钱琳");
        phoneName.put("15882276699", "缪静欣");
        phoneName.put("19694998088", "焦秋菊");
        phoneName.put("18264427294", "吕访琴");
        phoneName.put("17245432318", "沈丹");
        phoneName.put("16705495586", "褚美丽");
    }

    /**
     * 生成日志
     * @return
     */
    public  String produceLog() throws ParseException {
        String caller;
        String callee;
        String buildTime;
        //1.随机生成两个不同的电话号
        int callerIndex = (int) (Math.random() * phoneNum.size());
        caller = phoneNum.get(callerIndex);//获取其中一个
        while(true){
           int calleeIndex=(int)(Math.random()*phoneNum.size());
           callee=phoneNum.get(calleeIndex);
           if(callerIndex!=calleeIndex){
               break;
           }
        }

        //2随机生成通话建立的时间（start,end）
         buildTime = randomBuidTime(start, end);
        //String buildTime=
        //3随机生成通话时
        DecimalFormat dfs= new DecimalFormat("0000");
        String dura=dfs.format((int)(Math.random()*30*60+1));
        return caller+","+callee+","+buildTime+","+dura+"\n";

       // return null;
    }
    //生成通话时间
    private String randomBuidTime(String start,String end) throws ParseException {
        long startPoint = sdf1.parse(start).getTime();
        long endPoint = sdf1.parse(end).getTime();

        long resultTS = startPoint + (long) (Math.random() * (endPoint - startPoint));
        return sdf2.format(new Date(resultTS));

    }
    public void writeLog(String path) throws Exception {
        //   FileOutputStream fos = new FileOutputStream("C:\\Users\\cranen\\Desktop\\cc.csv");
        FileOutputStream fos = new FileOutputStream(path);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        while (true) {
            String log = produceLog();
            osw.write(log);
            osw.flush();//必须刷新和关闭流的时候值才能出来
            System.out.print(log);
            Thread.sleep(300);
        }


    }

    public static void main(String[] args) throws Exception {
        //DecimalFormat df = new DecimalFormat("0000");
        // String format = df.format(12);
        //System.out.println(format);
        Producers a = new Producers();
        a.init();
//      /**/  while (true){
        // a.init();
        //   System.out.println(a.produceLog());
        //   Thread.sleep(500);
        //  }
        //a.writeLog("C:\\Users\\cranen\\Desktop\\a.txt");
        a.writeLog(args[0]);


    }
}
