package read.history.oldVersionCode;

import read.Parameters;
import read.utils.FindByte;

import java.io.*;
import java.text.ParseException;
import java.util.Date;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/22 上午10:32
 */
public class ReadDateFromHead {

    public static Date readDataSegMentHead(File file) throws ParseException, IOException {
        //用于承装数据段头的字节

        byte[] dataSegmentHeadByte = new byte[34] ;
        //打开流
        BufferedInputStream buffered = new BufferedInputStream(new FileInputStream(file)) ;
        buffered.skip(Parameters.WenJianTou);
        buffered.read(dataSegmentHeadByte);
        buffered.close();
        byte[] segmentDate = FindByte.searchByteSeq(dataSegmentHeadByte, 8, 17) ;
        String startDate;
        startDate = "20" + segmentDate[0]+"-";
        startDate = startDate + segmentDate[1]+"-";
        startDate = startDate + segmentDate[2]+" ";
        startDate = startDate + segmentDate[3]+":";
        startDate = startDate + segmentDate[4]+":";
        startDate = startDate + segmentDate[5];

        Date d = String2Date.str2Date(startDate);

        return d;
    }

}
