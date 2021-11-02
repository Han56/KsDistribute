package read.utils;

import com.alibaba.fastjson.JSON;
import entity.HFMEDHead;
import net.minidev.json.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author han56
 * @description 功能描述 【HDFS】相关客户端操作
 * @create 2021/10/19 上午11:02
 */
public class HDFSUtils {
    private static FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException,IOException,InterruptedException{
        // 连接集群 nn 地址
        URI uri = new URI("hdfs://hadoop101:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","1");

        //用户
        String user = "han56";
        //获取客户端对象
        fileSystem = FileSystem.get(uri,configuration,user);
    }

    @After
    public void close() throws IOException{
        fileSystem.close();
    }

    /*
    * 读取文件方法
    * */
    public HFMEDHead readHFileHead(String pathStr) throws IOException {
        Path path = new Path(pathStr);

        //open file
        FSDataInputStream fsDataInputStream = fileSystem.open(path);

        byte[] tempByte = new byte[186];

        int byteLenght = fsDataInputStream.read(tempByte);

//        System.out.println(new String(temByte,0,byteLenght));
        /*
        * 数据赋值
        * */
        byte[] fileHeadLengthByte = FindByte.searchByteSeq(tempByte,0,1);
        byte[] formatVerByte = FindByte.searchByteSeq(tempByte, 2, 5) ;

        byte[] dataFileNameByte = FindByte.searchByteSeq(tempByte, 6, 85) ;

        byte[] operatorNameByte = FindByte.searchByteSeq(tempByte, 86, 95) ;

        byte[] palaceNameByte = FindByte.searchByteSeq(tempByte, 96, 115) ;

        byte[] startDateByte = FindByte.searchByteSeq(tempByte, 116, 125) ;

        byte[] sysCounterByte = FindByte.searchByteSeq(tempByte, 126, 133) ;

        byte[] sysFeqByte = FindByte.searchByteSeq(tempByte, 134, 141) ;

        byte[] uesrIdNameByte = FindByte.searchByteSeq(tempByte, 142, 149) ;

        byte[] adFedByte = FindByte.searchByteSeq(tempByte, 150, 153) ;

        byte[] resolutionByte = FindByte.searchByteSeq(tempByte, 154, 155) ;

        byte[] fileDurationByte = FindByte.searchByteSeq(tempByte, 156, 159) ;

        byte[] segmentNumByte = FindByte.searchByteSeq(tempByte, 160, 163) ;

        byte[] segmentHeadLengthByte = FindByte.searchByteSeq(tempByte, 164, 165) ;

        byte[] indexSegmentHeadLenghtByte = FindByte.searchByteSeq(tempByte, 166, 167) ;

        byte[] segmentRecNumByte = FindByte.searchByteSeq(tempByte, 168, 171) ;

        byte[] segmentDurationByte = FindByte.searchByteSeq(tempByte, 172, 175) ;

        byte[] featureCodeByte = FindByte.searchByteSeq(tempByte, 176, 179) ;

        byte[] channelOnNumByte = FindByte.searchByteSeq(tempByte, 180, 181) ;

        byte[] reserveByte = FindByte.searchByteSeq(tempByte, 182, 185) ;

        /*
        * 数据转换
        * */
        short fileHeadLength = Byte2OtherDataFormat.byte2Short(fileHeadLengthByte);
        String formatVer = Byte2OtherDataFormat.byte2String(formatVerByte);
        String dataFileName = Byte2OtherDataFormat.byte2String(dataFileNameByte);
        String operator = Byte2OtherDataFormat.byte2String(operatorNameByte);
        String palaceName = Byte2OtherDataFormat.byte2String(palaceNameByte);

        /*
        * 时间数据转换为String类型
        * */
        String startDate = Byte2OtherDataFormat.byte2String(startDateByte);
        String sysCounter = Byte2OtherDataFormat.byte2String(sysCounterByte);
        String sysFeq = Byte2OtherDataFormat.byte2String(sysFeqByte);

        /*
        * 其他数据转换
        * */
        String userIdName = Byte2OtherDataFormat.byte2String(uesrIdNameByte);
        int adFre = Byte2OtherDataFormat.byte2Int(adFedByte);
        short resolution = Byte2OtherDataFormat.byte2Short(resolutionByte);
        int segmentNum = Byte2OtherDataFormat.byte2Int(segmentNumByte);
        short segmentHeadLength = Byte2OtherDataFormat.byte2Short(segmentHeadLengthByte);
        short indexSegmentHeadLength = Byte2OtherDataFormat.byte2Short(indexSegmentHeadLenghtByte);
        int segmentRecNum = Byte2OtherDataFormat.byte2Int(segmentRecNumByte);
        String featureCode = Byte2OtherDataFormat.byte2String(featureCodeByte);
        short channelOnNum = Byte2OtherDataFormat.byte2Short(channelOnNumByte);

        /*
        * set bean 对象
        * */
        HFMEDHead hfmedHead = new HFMEDHead();
        hfmedHead.setAdFre(adFre);
        hfmedHead.setChannelOnNum(channelOnNum);
        hfmedHead.setDataFileName(dataFileName);
        hfmedHead.setFeatureCode(featureCode);
        hfmedHead.setFileHeadLength(fileHeadLength);
        hfmedHead.setFormatVer(formatVer);
        hfmedHead.setIndexSegmentHeadLength(indexSegmentHeadLength);
        hfmedHead.setOperator(operator);
        hfmedHead.setPalaceName(palaceName);
        hfmedHead.setResolution(resolution);
        hfmedHead.setSegmentHeadLength(segmentHeadLength);
        hfmedHead.setSegmentRecNum(segmentRecNum);
        hfmedHead.setSegmentNum(segmentNum);
        hfmedHead.setUserIdName(userIdName);
        return hfmedHead;
    }

    @Test
    public void testReadHFHead() throws IOException {
        String pathStr = "hdfs://hadoop101:8020/hy_history_data/September/S/Test_190925110520.HFMED";
        HDFSUtils utils = new HDFSUtils();
        HFMEDHead hfmedHead = utils.readHFileHead(pathStr);
        System.out.println(JSON.toJSONString(hfmedHead));
    }

}
