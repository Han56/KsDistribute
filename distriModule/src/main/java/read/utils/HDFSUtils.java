package read.utils;

import com.alibaba.fastjson.JSON;
import entity.ChannelInfo;
import entity.HFMEDHead;
import entity.HfmedSegmentHead;
import impl.ReadFileImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author han56
 * @description 功能描述 【HDFS】相关客户端操作
 * @create 2021/10/19 上午11:02
 */
public class HDFSUtils implements ReadFileImpl {
    private static FileSystem fileSystem;

    public HDFSUtils() throws IOException {
    }

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

    @Test
    public void testReadHFHead() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(
                new Path("hdfs://hadoop101:8020/hy_history_data/September/S/Test_190925110520.HFMED"));
        HDFSUtils utils = new HDFSUtils();
        //打印前300Byte测试
        //printByteFile(300,fsDataInputStream);
        System.out.println("文件头输出测试：");
        HFMEDHead resHead = utils.readFileHead(fsDataInputStream);
        System.out.println(JSON.toJSONString(resHead));
        System.out.println("通道信息输出测试：");
        ChannelInfo[] channelInfos = utils.getChannelInfoByFile(fsDataInputStream,resHead.getChannelOnNum());
        System.out.println(JSON.toJSONString(channelInfos));
        System.out.println("第一个数据段输出测试：");
        HfmedSegmentHead hfmedSegmentHead = utils.getDataHeadInfoByFile(fsDataInputStream,0, resHead.getChannelOnNum());
        System.out.println(JSON.toJSONString(hfmedSegmentHead));
        fsDataInputStream.close();
    }

    /*
    * 打印文件 测试使用
    * */
    public static void printByteFile(int skipLength,FSDataInputStream fsDataInputStream) throws IOException {
        byte[] testPrint = new byte[skipLength];
        int l=fsDataInputStream.read(testPrint);
    }

    /*
    * 读取文件头操作方法
    * */
    @Override
    public HFMEDHead readFileHead(FSDataInputStream fsDataInputStream) throws IOException {

        byte[] tempByte = new byte[186];

        int byteLenght = fsDataInputStream.read(tempByte);
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
        String fileDurationStr = Byte2OtherDataFormat.byte2String(fileDurationByte);
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

    /*
    * 读取通道信息操作方法
    * */
    @Override
    public ChannelInfo[] getChannelInfoByFile(FSDataInputStream fsDataInputStream,short channelOnNum) throws IOException {

        ChannelInfo[] channelInfos = new ChannelInfo[channelOnNum];
        for (int i=0;i<channelOnNum;i++)
            channelInfos[i] = new ChannelInfo();

        // 将读到的通道信息放到二维的数组中
        for (int i=0;i<channelOnNum;i++){
            byte[] sensorArray = new byte[14];
            int byteArrayLength = fsDataInputStream.read(sensorArray);
            //获取字节序列
            byte[] chNoByte = FindByte.searchByteSeq(sensorArray, 0, 1);
            byte[] chNameByte = FindByte.searchByteSeq(sensorArray, 2, 5);
            byte[] chUnitByte = FindByte.searchByteSeq(sensorArray, 6, 9);
            byte[] chCaliByte = FindByte.searchByteSeq(sensorArray, 10, 13);
            byte[] tempFloat ={chCaliByte[3] ,chCaliByte[2] ,chCaliByte[1] ,chCaliByte[0]} ;

            //解析字符序列
            short chNo = Byte2OtherDataFormat.byte2Short(chNoByte);
            String chName = Byte2OtherDataFormat.byte2String(chNameByte);
            String chUnit = Byte2OtherDataFormat.byte2String(chUnitByte);
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(tempFloat));
            float fchCali = dis.readFloat();
            dis.close();

            channelInfos[i].setChName(chName);
            channelInfos[i].setChNo(chNo);
            channelInfos[i].setChUnit(chUnit);
            channelInfos[i].setChCali(fchCali);

        }
        return channelInfos;
    }

    /*
     * 读取数据段头操作方法
     * */
    @Override
    public HfmedSegmentHead getDataHeadInfoByFile(FSDataInputStream fsDataInputStream,int segmentNum,short channelOnNum) throws IOException {

        //根据channelOnNum计算要跳过多少字节
        /*
        * 文件头186
        *     +
        * channelOnNum*14
        * */
        //存放数据段头信息
        byte[] segHeadData = new byte[34];
        long readLen=fsDataInputStream.read(segHeadData);
        byte[] featureCodeByte = FindByte.searchByteSeq(segHeadData,0,3);
        byte[] segmentNoByte = FindByte.searchByteSeq(segHeadData,4,7);
        byte[] segmentDateByte = FindByte.searchByteSeq(segHeadData,8,17);

        //转换二进制
        int segmentNo = Byte2OtherDataFormat.byte2Int(segmentNoByte);
        String segmentDateStr = "20"+segmentDateByte[0]+"-"+segmentDateByte[1]+"-"+segmentDateByte[2]+" "
                +segmentDateByte[3]+":"+segmentDateByte[4]+":"+segmentDateByte[5];
        String featureCodeStr = Byte2OtherDataFormat.byte2String(featureCodeByte);

        //封装序列化
        HfmedSegmentHead hfmedSegmentHead = new HfmedSegmentHead();
        hfmedSegmentHead.setSegmentNo(segmentNo);
        hfmedSegmentHead.setFeatureCode(featureCodeStr);
        hfmedSegmentHead.setSysTime(segmentDateStr);
        return hfmedSegmentHead;
    }



}
