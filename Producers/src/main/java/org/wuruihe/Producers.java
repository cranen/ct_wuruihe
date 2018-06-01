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

    public static final String E = "15064792307";
    List<String> phoneNum = new ArrayList<String>();
    Map<String, String> phoneName = new HashMap<String,String>();

    /**
     * 添加数据
     */
    public void init() {
        phoneNum.add("15369468720");
        phoneNum.add("19920860202");
        phoneNum.add("18411925860");
        phoneNum.add("14473548449");
        phoneNum.add("18749966182");
        phoneNum.add("19379884788");
        phoneNum.add("19335715448");
        phoneNum.add("18503558939");
        phoneNum.add("13407209608");
        phoneNum.add("15596505995");
        phoneNum.add("17519874292");
        phoneNum.add("15178485516");
        phoneNum.add("19877232369");
        phoneNum.add("18706287692");
        phoneNum.add("18944239644");
        phoneNum.add("17325302007");
        phoneNum.add("18839074540");
        phoneNum.add("19879419704");
        phoneNum.add("16480981069");
        phoneNum.add("18674257265");
        phoneNum.add("18302820904");
        phoneNum.add("15133295266");
        phoneNum.add("17868457605");
        phoneNum.add("15490732767");
        phoneNum.add(E);

        phoneName.put("15369468720", "李雁");
        phoneName.put("19920860202", "卫艺");
        phoneName.put("18411925860", "仰莉");
        phoneName.put("14473548449", "陶欣悦");
        phoneName.put("18749966182", "施梅梅");
        phoneName.put("19379884788", "金虹霖");
        phoneName.put("19335715448", "魏明艳");
        phoneName.put("18503558939", "华贞");
        phoneName.put("13407209608", "华啟倩");
        phoneName.put("15596505995", "仲采绿");
        phoneName.put("17519874292", "卫丹");
        phoneName.put("15178485516", "戚丽红");
        phoneName.put("19877232369", "何翠柔");
        phoneName.put("18706287692", "钱溶艳");
        phoneName.put("18944239644", "钱琳");
        phoneName.put("17325302007", "缪静欣");
        phoneName.put("18839074540", "焦秋菊");
        phoneName.put("19879419704", "吕访琴");
        phoneName.put("16480981069", "沈丹");
        phoneName.put("18674257265", "褚美丽");
        phoneName.put("18302820904", "孙怡");
        phoneName.put("15133295266", "许婵");
        phoneName.put("17868457605", "曹红恋");
        phoneName.put("15490732767", "吕柔");
        phoneName.put("15064972307", "冯怜云");
    }

    /**
     * 生成日志
     * @return
     */
    public  String produceLog() throws ParseException {
        String caller;//随机数空间
        String callee;
        String buildTime;
        String duration;
        //1、随机生成两个不同的电话号码
        int callerIndex=(int)(Math.random()*phoneNum.size());
        caller=phoneNum.get(callerIndex);//获取其中一个生成的电话数
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
        return caller+","+callee+","+buildTime+dura+"\n";

       // return null;
    }
    //生成通话时间
    private String randomBuidTime(String start,String end) throws ParseException {
        SimpleDateFormat sf=new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdfd2=new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        long startpoit = sf.parse(start).getTime();
        long endpoit = sf.parse(end).getTime();
        long result = startpoit + (int) (Math.random() * (endpoit - startpoit));
        return sdfd2.format(new Date(result));

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
        a.writeLog("C:\\Users\\cranen\\Desktop\\a.txt");


    }
}
