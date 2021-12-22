package read.history.oldVersionCode;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/22 上午10:34
 */
public class String2Date {

    /*
     * 字符串转化为日期
     */
    public static Date str2Date(String dateStr) throws  ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss") ;

        return simpleDateFormat.parse(dateStr);
    }

    public static Date str2Date1(String dateStr) throws ParseException{

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;

        return simpleDateFormat.parse(dateStr);
    }

    public static Date str2Date2(String dateStr) throws ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        return simpleDateFormat.parse(dateStr);
    }

}
