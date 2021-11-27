package read.utils;

import com.alibaba.fastjson.JSON;
import entity.ChannelInfo;
import entity.DataElement;
import entity.HFMEDHead;
import entity.HfmedSegmentHead;
import impl.ExceptionalImp;
import impl.ReadFileImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import read.Parameters;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author han56
 * @description 功能描述 【HDFS】相关客户端操作
 * @create 2021/10/19 上午11:02
 */
public class HDFSUtils implements ReadFileImpl, ExceptionalImp {
    private static FileSystem fileSystem;

    boolean isBroken = false;
    boolean isVoltProcess = false;
    boolean flag1 = false;
    boolean flag2 = false;
    public int timeCount = 0;
    int loopCount = 0;

    /*
    * 测试数据
    * */
    List<String> filesPath = new ArrayList<>();

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
    public void testReadHFHead() throws IOException, InterruptedException, ParseException {
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/S/Test_190925110520.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/Z/Test_190925103745.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/U/Test_190925105915.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/V/Test_190925100949.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/Y/Test_190925101650.HFMED");
        filesPath.add("hdfs://hadoop101:8020/hy_history_data/September/T/Test_190925104506.HFMED");

        DateUtils dateUtils = new DateUtils();
        List<String> startAndEndTime = dateUtils.getStartAndEndTime(filesPath);
        System.out.println("window start:"+startAndEndTime.get(0)+"window end:"+startAndEndTime.get(1));
        String winStart = startAndEndTime.get(0);String winEnd = startAndEndTime.get(1);

        for (String path:filesPath){
            System.out.println("开始读取文件："+path);
            FSDataInputStream fsDataInputStream = fileSystem.open(
                    new Path(path));
            HDFSUtils utils = new HDFSUtils();
            System.out.println("文件头输出测试：");
            HFMEDHead resHead = utils.readFileHead(fsDataInputStream);
            System.out.println(JSON.toJSONString(resHead));
            System.out.println("通道信息输出测试：");
            ChannelInfo[] channelInfos = utils.getChannelInfoByFile(fsDataInputStream,resHead.getChannelOnNum());
            System.out.println(JSON.toJSONString(channelInfos));
            HfmedSegmentHead hfmedSegmentHead = null;
            DataElement resData;
            int by=-1;
            boolean fileIsOver = false;
            loopCount = 0;
            //读数据段头和数据段
            while (true){
                /*
                 * 读之前需要判断是否到达了末尾
                 * */
                byte[] preRead = new byte[4];
                fsDataInputStream.read(preRead);
                byte[] featureCode = {preRead[0],preRead[1],preRead[2],preRead[3]};
                String isHFME = new String(featureCode);
                System.out.println("isHFME?  "+isHFME);
                if (isHFME.equals("HFME")){
                    loopCount=0;
                    timeCount=0;
                    //读数据段头
                    /*
                     * 如果时间等于 每一组文件 的起点时间则开始读取，否则 continue
                     * 跳出循环条件：如果时间等于 每组文件的结束时间 则break while
                     * */
                    hfmedSegmentHead=getDataHeadInfoByFile(fsDataInputStream,preRead, resHead.getChannelOnNum());
                    //方便调试，遇到特征码停一秒
                    Thread.sleep(2000);
                }else {
                    if (isVoltProcess){
                        loopCount=0;
                        isVoltProcess=false;
                    }
                    loopCount++;
                    resData = getDataInfoByFile(fsDataInputStream,preRead, resHead.getChannelOnNum(), loopCount);
                    String formerDateStr = formerDate(hfmedSegmentHead.getSysTime(),timeCount);
                    resData.setDataCalendar(formerDateStr);
                    /*
                    * 如果该段时间小于窗口起点，则跳过不存储
                    * 如果该段时间大于窗口起点，存储
                    * 如果该段时间大于窗口结点，break
                    * */
                    if (dateUtils.segTimeCompareToWinStartTime(formerDateStr,winStart))
                        System.out.println("数据合法，存储");
                    if (dateUtils.segTimeCompareToWinEndTime(formerDateStr,winEnd)){
                        System.out.println("该文件结束，break");
                        break;
                    }
                    System.out.println(JSON.toJSONString(resData));
                }

            }
            fsDataInputStream.close();
        }

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
    public HfmedSegmentHead getDataHeadInfoByFile(FSDataInputStream fsDataInputStream,byte[] preRead,short channelOnNum) throws IOException {

        //根据channelOnNum计算要跳过多少字节
        /*
        * 文件头186
        *     +
        * channelOnNum*14
        * */
        //存放数据段头信息
        byte[] segHeadData = new byte[34];
        byte[] rearRead = new byte[30];
        fsDataInputStream.read(rearRead);
        //进行拼接
        System.arraycopy(preRead,0,segHeadData,0,preRead.length);
        System.arraycopy(rearRead,0,segHeadData,preRead.length,rearRead.length);
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
        //print test
        System.out.println(JSON.toJSONString(hfmedSegmentHead));
        return hfmedSegmentHead;
    }

    /*
    * 读取数据段重写方法
    * */
    @Override
    public DataElement getDataInfoByFile(FSDataInputStream fsDataInputStream,byte[] preRead,short channelOnNum,int loopCount) throws IOException {
        int byteNum,voltStart,voltEnd;
        short volt;
        DataElement dataElement = new DataElement();
        //根据通道数设置读取的字节数
        if (channelOnNum==7){
            byteNum=14;voltStart=12;voltEnd=13;
            byte[] data = new byte[byteNum];
            byte[] rearRead = new byte[10];
            fsDataInputStream.read(rearRead);
            //进行拼接
            System.arraycopy(preRead,0,data,0,preRead.length);
            System.arraycopy(rearRead,0,data,preRead.length,rearRead.length);
            short x1 = Byte2OtherDataFormat.byte2Short(data[0], data[1]);
            short y1 = Byte2OtherDataFormat.byte2Short(data[2], data[3]);
            short z1 = Byte2OtherDataFormat.byte2Short(data[4], data[5]);
            short x2 = Byte2OtherDataFormat.byte2Short(data[6], data[7]);
            short y2 = Byte2OtherDataFormat.byte2Short(data[8], data[9]);
            short z2 = Byte2OtherDataFormat.byte2Short(data[10], data[11]);

            dataElement.setX1(x1);
            dataElement.setY1(y1);
            dataElement.setZ1(z1);

            dataElement.setX2(x2);
            dataElement.setY2(y2);
            dataElement.setZ2(z2);
            volt = Byte2OtherDataFormat.byte2Short(data[voltStart],data[voltEnd]);
        }
        else if (channelOnNum==4){
            byteNum=8;voltStart=6;voltEnd=7;
            byte[] data = new byte[byteNum];
            byte[] rearRead = new byte[4];
            fsDataInputStream.read(rearRead);
            //进行拼接
            System.arraycopy(preRead,0,data,0,preRead.length);
            System.arraycopy(rearRead,0,data,preRead.length,rearRead.length);
            short x2=Byte2OtherDataFormat.byte2Short(data[0],data[1]);
            short y2=Byte2OtherDataFormat.byte2Short(data[2],data[3]);
            short z2=Byte2OtherDataFormat.byte2Short(data[4],data[5]);
            dataElement.setX2(x2);dataElement.setY2(y2);dataElement.setZ2(z2);
            volt = Byte2OtherDataFormat.byte2Short(data[voltStart],data[voltEnd]);
        }
        else{
            System.out.println("通道信息数目异常,结束读取,数值为:"+channelOnNum);
            return null;
        }
        //高低电平判断是否一秒结束
        if (voltProcessing(volt,loopCount)){
            isVoltProcess = true;
        }
        return dataElement;
    }

    /*
    * 重写异常处理方法
    * */

    /*
    * 解决每个频率周期数据结束
    * 根据timeCount即可得到下一秒的数据
    * */
    @Override
    public String formerDate(String segHeadDate,int timeCount) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 24小时制
        //引号里面个格式也可以是 HH:mm:ss或者HH:mm等等，很随意的，不过在主函数调用时，要和输入的变
        //量day格式一致
        Date date = null;
        try {
            date = format.parse(segHeadDate);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if (date == null)
            return "";
        System.out.println("front:" + format.format(date)); //显示输入的日期
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.SECOND, timeCount);// 24小时制
        date = cal.getTime();
        System.out.println("after:" + format.format(date));  //显示更新后的日期
        return format.format(date);
    }

    /*
    * 尾部处理方法
    * 一个文件读到末尾后，关闭数据流
    * */
    @Override
    public void tailOfflineProcess() {

    }

    /*
    * 高低电平——一秒数据段结束标志
    * */
    @Override
    public boolean voltProcessing(short voltValue, int loopCount) {
        if (!isBroken){
            if (loopCount>(Parameters.FREQUENCY+210)){
                isBroken=true;
                System.out.println("该文件出现GPS缺失");
                timeCount++;
                flag1=flag2=false;
                return true;
            }
            //判断1s是否结束，结束跳出while
            if (Math.abs(voltValue)<1000)
                flag2=true;
            if (Math.abs(voltValue)>5000&&flag2)
                flag1=true;
            //高电平结束，说明1s数据结束
            if (flag1&&flag2){
                timeCount++;
                flag1=flag2=false;
                return true;
            }
        }else {
            //在对齐时就出现电压缺失，直接到这部分
            if (loopCount>=(Parameters.FREQUENCY+200)){
                timeCount++;
                flag1=flag2=false;
                return true;
            }
        }
        return false;
    }
}
