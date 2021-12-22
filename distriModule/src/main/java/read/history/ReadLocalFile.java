package read.history;

import com.alibaba.fastjson.JSON;
import entity.ChannelInfo;
import entity.DataElement;
import entity.HFMEDHead;
import org.junit.Test;
import read.Parameters;
import read.history.oldVersionCode.ReadDateFromHead;
import read.history.oldVersionCode.ReadHfmedHead;
import read.history.oldVersionCode.ReadSensorProperties;
import read.utils.Byte2OtherDataFormat;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/22 上午9:35
 */
public class ReadLocalFile {

    private File file;
    /** 数据段总数 */
    private int segmentNum;
    /** 每个数据段中数据的个数 */
    private int segmentRecNum;
    /** 通道个数 */
    private int channelNum;
    /** 通道个数字符串用于读取 */
    private int channel;
    /** 数据头、文件头、字节数、电压起始、电压结束 */
    private int datahead;
    private int bytenum;
    private int voltstart;
    private int voltend;
    boolean flag1 = false;
    boolean flag2 = false;
    /** 第一条数据的日期 */
    private Date date = new Date();
    /** 通道单位大小 */
    private float chCahi;
    /** 存放文件的字节 */
    private byte[] dataByte;
    /** 缓冲池大小，10个传感器*频率*10s时间。 */
    private int bufferPoolSize = 10 * (Parameters.FREQUENCY + 200) * 10;
    /** 流的重定向 */
    private BufferedInputStream buffered;
    /** when GPS signal has gone, its value become true */
    public boolean isBroken = false;
    /** 秒数计数器 , 每调用一次getData的时候 ，这个计数器就加一 ，表示加一秒 */
    public int timeCount = 0;

    @Test
    public void readLocal() throws IOException, ParseException {
        this.file = new File("/data/files/DownLoads/KsDisIn/Test_190925105915.HFMED");
        HFMEDHead hfmedHead = new ReadHfmedHead().readHead(file);
        this.settings(hfmedHead);
        this.date = ReadDateFromHead.readDataSegMentHead(file);
        ChannelInfo[] sensor = new ReadSensorProperties().readSensor(file);
        this.chCahi = sensor[0].getChCali();
        dataByte=new byte[this.bytenum];
        this.buffered = new BufferedInputStream(new FileInputStream(file),bufferPoolSize);
        buffered.read(new byte[Parameters.WenJianTou]);
        System.out.println(JSON.toJSONString(hfmedHead));
        System.out.println("日期信息:"+this.date);
        System.out.println("单位信息:"+this.chCahi);
        for (int i=0;i<200;i++)
           readLocalDataOffLine();
    }

    public void settings(HFMEDHead hfmedHead){
        this.segmentNum = hfmedHead.getSegmentNum();// 从文件头中获得段的数量
        this.segmentRecNum = hfmedHead.getSegmentRecNum();// 获得每个段的数据条目数
        this.channelNum = hfmedHead.getChannelOnNum();

        if (channelNum == 7) {
            this.channel = 123456;
            this.datahead = 20;
            this.bytenum = 14;
            this.voltstart = 12;
            this.voltend = 13;
        } else if (channelNum == 4) {
            this.channel = 456;
            this.datahead = 26;
            this.bytenum = 8;
            this.voltstart = 6;
            this.voltend = 7;
        }
    }

    /*
    * 旧版本读取一秒钟的数据
    * */
    public void readLocalDataOffLine() throws IOException {
        int by = -1;
        boolean fileIsOver = false;
        int loopCount = 0;
        List<String> data = new ArrayList<>();
        short volt=0;
        while (true){
            try {
                if (!fileIsOver){
                    if ((by=buffered.read(dataByte))<dataByte.length){
                        fileIsOver=true;
                        Thread.sleep(2000);
                        continue;
                    }
                }else {
                    if (by!=-1){
                        buffered.skip(this.bytenum-by);
                        fileIsOver=false;
                        continue;
                    }
                    else {
                        if (buffered.read(dataByte)==-1){
                            return;
                        }
                    }
                }
            }catch (IOException | InterruptedException e){
                e.printStackTrace();
                return;
            }
            loopCount++;
            byte[] feature = {dataByte[0],dataByte[1],dataByte[2],dataByte[3]};
            if (new String(feature).compareTo("HFME")==0){
                buffered.skip(this.datahead);
                buffered.read(dataByte);
            }
            DataElement dataElement = this.getDataElementFromDataBytes();
            dataElement.setDataCalendar(this.formerDate());
            System.out.println(JSON.toJSONString(dataElement));
            data.add(dataElement.toString());
            volt=Byte2OtherDataFormat.byte2Short(dataByte[this.voltstart],dataByte[this.voltend]);
            if (this.voltProcessing(volt,loopCount))
                break;
        }
    }

    public DataElement getDataElementFromDataBytes(){
        DataElement dataElement = new DataElement();
        if (channel==456){
            short x2 = Byte2OtherDataFormat.byte2Short(dataByte[0],dataByte[1]);
            short y2 = Byte2OtherDataFormat.byte2Short(dataByte[2],dataByte[3]);
            short z2 = Byte2OtherDataFormat.byte2Short(dataByte[4],dataByte[5]);
            dataElement.setX2(x2);
            dataElement.setY2(y2);
            dataElement.setZ2(z2);
        }
        if (channel==123456){
            short x1 = Byte2OtherDataFormat.byte2Short(dataByte[0],dataByte[1]);
            short y1 = Byte2OtherDataFormat.byte2Short(dataByte[2],dataByte[3]);
            short z1 = Byte2OtherDataFormat.byte2Short(dataByte[4],dataByte[5]);
            short x2 = Byte2OtherDataFormat.byte2Short(dataByte[6],dataByte[7]);
            short y2 = Byte2OtherDataFormat.byte2Short(dataByte[8],dataByte[9]);
            short z2 = Byte2OtherDataFormat.byte2Short(dataByte[10],dataByte[11]);
            dataElement.setX1(x1);
            dataElement.setY1(y1);
            dataElement.setZ1(z1);
            dataElement.setX2(x2);
            dataElement.setY2(y2);
            dataElement.setZ2(z2);
        }
        return dataElement;
    }

    public String formerDate(){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(this.date);
        calendar.add(Calendar.SECOND,timeCount);
        Date startDate1 = calendar.getTime();
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");
        return format2.format(startDate1);
    }

    public boolean voltProcessing(short volt,int loopCount){
        if (!isBroken){
            if (loopCount>(Parameters.FREQUENCY+210)){
                isBroken=true;
                System.out.println("GPS缺失");
                timeCount++;
                flag1=flag2=false;
                return true;
            }
            if (Math.abs(volt)<1000)
                flag2=true;
            if (Math.abs(volt)>5000&&flag2)
                flag1=true;
            if (flag1&&flag2){
                timeCount++;
                flag2=flag1=false;
                return true;
            }else {
                if (loopCount>=(Parameters.FREQUENCY+200)){
                    timeCount++;
                    flag1=flag2=false;
                    return true;
                }
            }
        }
        return false;
    }


}
