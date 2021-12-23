package read.utils;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author han56
 * @description 功能描述
 * 时间处理工具
 * @create 2021/11/24 下午4:26
 */
public class DateUtils {


    /*
    * 获取一组文件的时间
    * 且确定滑动时间窗口的 起止时间
    * */
    public List<String> getStartAndEndTime(List<String> filesPath){
        List<String> dateArray = new ArrayList<>();
        /*
         * 获取文件路径中的起始时间与终止时间
         * */
        for (String str:filesPath){
            String timeStr = "20"+str.substring(7,9)+"-"+str.substring(9,11)+"-"+str.substring(11,13)
                    +" "+str.substring(13,15)+":"+str.substring(15,17)+":"+str.substring(17,19);
            dateArray.add(timeStr);
        }
        System.out.println(dateArray);
        Map<String, Integer> dateMap = new TreeMap<>();
        for(String dateKey:dateArray){
            if(dateMap.containsKey(dateKey)){
                int value = dateMap.get(dateKey) + 1;
                dateMap.put(dateKey, value);
            }else{
                dateMap.put(dateKey, 1);
            }
        }
        Set<String> keySet = dateMap.keySet();
        String []sorttedArray = new String[keySet.size()];
        Iterator<String> iter = keySet.iterator();
        int index = 0;
        while (iter.hasNext()) {
            String key = iter.next();
            sorttedArray[index++] = key;
        }
        int sorttedArrayLen = sorttedArray.length;
        System.out.println("最小日期是：" + sorttedArray[0]);
        System.out.println("最大日期是：" + sorttedArray[sorttedArrayLen - 1]);

        //即可推出整个数据读取期间的时间窗口
        String windowStartTime = sorttedArray[sorttedArrayLen - 1];
        String windowEndTime = addDateHour(sorttedArray[0], 1);
        System.out.println("滑动窗口起始时间："+windowStartTime);
        System.out.println("滑动窗口结束时间："+windowEndTime);
        List<String> res = new ArrayList<>();
        res.add(windowStartTime);
        res.add(windowEndTime);
        return res;
    }

    /*
    * 返回的是字符串型的时间，输入的
    * 是String day, int x
    * 增加 x 小时
    * */
    public static String addDateHour(String day, int x){

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 24小时制
        //引号里面个格式也可以是 HH:mm:ss或者HH:mm等等，很随意的，不过在主函数调用时，要和输入的变
        //量day格式一致
        Date date = null;
        try {
            date = format.parse(day);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if (date == null)
            return "";
        System.out.println("front:" + format.format(date)); //显示输入的日期
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, x);// 24小时制
        date = cal.getTime();
        System.out.println("after:" + format.format(date));  //显示更新后的日期
        return format.format(date);

    }

    /*
    * 当前时间与窗口起始时间比较方法
    * */
    public boolean segTimeCompareToWinStartTime(String segDateStr,String winStartDateStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
        Date segDate = format.parse(segDateStr);
        Date winStartDate = format.parse(winStartDateStr);
        return segDate.compareTo(winStartDate) >= 0;
    }

    /*
    * 当前时间与窗口结束时间比较方法
    * */
    public boolean segTimeCompareToWinEndTime(String segDateStr,String winEndDateStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
        Date segDate = format.parse(segDateStr);
        Date winEndDate = format.parse(winEndDateStr);
        return segDate.compareTo(winEndDate) > 0;
    }

    @Test
    public void testSegDateCompareFunc() throws ParseException {
        String winStart = "2019-09-2511:05:20";
        String winEnd ="2019-09-2511:09:49";
        DateUtils dateUtils = new DateUtils();
        boolean b = dateUtils.segTimeCompareToWinStartTime("2019-09-2510:05:20", winStart);
        System.out.println(b);
        boolean b1 = dateUtils.segTimeCompareToWinEndTime("2019-9-2511:09:50", winEnd);
        System.out.println(b1);
    }

}
