package read.utils;

import entity.ChannelInfo;
import entity.DataElement;
import entity.HFMEDHead;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import read.Parameters;
import read.history.ReadLocalFile;
import read.history.oldVersionCode.ReadDateFromHead;
import read.history.oldVersionCode.ReadHfmedHead;
import read.history.oldVersionCode.ReadSensorProperties;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author han56
 * @description 功能描述
 * @create 2022/5/5 下午1:54
 */
public class ReadFileThread implements Runnable{

    //二进制文件完整路径
    private String totalFilePath;
    //HBase对应列名 即盘符字符串
    private String qualify;
    /** 通道个数 */
    private int channelNum;
    /** 数据段总数 */
    private int segmentNum;
    /** 每个数据段中数据的个数 */
    private int segmentRecNum;
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
    /** 流的重定向 */
    private BufferedInputStream buffered;
    /** 存放文件的字节 */
    private byte[] dataByte;
    /** when GPS signal has gone, its value become true */
    public boolean isBroken = false;
    /** 秒数计数器 , 每调用一次getData的时候 ，这个计数器就加一 ，表示加一秒 */
    public int timeCount = 0;
    /*
    * 窗口起止时间
    * */
    private String winStart;

    private String winEnd;
    /** 缓冲池大小，10个传感器*频率*10s时间。 */
    private int bufferPoolSize = 10 * (Parameters.FREQUENCY + 200) * 10;

    private Table table;

    //定义锁
    private final Object lock;

    //定义线程结束标志
    private  boolean stopped = true;

    private ThreadPoolExecutor threadPoolExecutor;

    //设置构造函数初始化
    public ReadFileThread(String totalFilePath, String qualify, String winStart, String winEnd, Table table,Object lock,ThreadPoolExecutor threadPoolExecutor) throws InterruptedException {
        this.totalFilePath = totalFilePath;
        this.qualify = qualify;
        this.winEnd = winEnd;
        this.winStart = winStart;
        this.table = table;
        this.lock = lock;
        this.threadPoolExecutor = threadPoolExecutor;
        System.out.println("count:  "+ReadLocalFile.count);
        int activeCount = threadPoolExecutor.getActiveCount();
        System.out.println("当前活跃线程数："+activeCount);
        Thread.sleep(8000);
    }

    //开启一个读二进制任务
    @Override
    public void run() {
        File file = new File(totalFilePath);
        try {
            HFMEDHead hfmedHead = new ReadHfmedHead().readHead(file);
            this.settings(hfmedHead);
            this.date = ReadDateFromHead.readDataSegMentHead(file);
            ChannelInfo[] sensor = new ReadSensorProperties().readSensor(file);
            this.chCahi = sensor[0].getChCali();
            dataByte=new byte[this.bytenum];
            this.buffered = new BufferedInputStream(new FileInputStream(file),bufferPoolSize);
            buffered.read(new byte[Parameters.WenJianTou]);

            //HBaseApi hBaseApi = new HBaseApi();
            DateUtils dateUtils = new DateUtils();
            while (stopped){
                List<DataElement> dataElementList = readLocalDataOffLine();
                String dataInnerTime = dataElementList.get(0).getDataCalendar();
                if (!dateUtils.segTimeCompareToWinStartTime(dataInnerTime,winStart)){
                    //System.out.println("["+qualify+"]"+" 正在寻找开始位置");
                    continue;
                }
                //此时时间戳 已经大于规定时间 将当前线程关闭 线程池 线程总数减少 并通知主线程 增加新的子线程
                if (dateUtils.segTimeCompareToWinEndTime(dataInnerTime,winEnd)){
                    System.out.println("read over,end timestamp:"+dataInnerTime);
                    //判断当前 count 是否小于6 如果小于则 执行同步代码块 解锁
                    synchronized (lock){
                        System.out.println("已发出继续循环通知");
                        ReadLocalFile.count--;
                        lock.notify();
                        Thread.sleep(3000);
                    }
                    stopped = false;
                    System.out.println("结束当前线程");
                }
                if (dateUtils.segTimeCompareToWinStartTime(dataInnerTime,winStart)){
                    System.out.println("rowkey:"+dataElementList.get(0).getDataCalendar()+" qualify:"+qualify+" total data size:"+dataElementList.size());
                    long currentTime = System.currentTimeMillis();
                    //旧版本阻塞存储方法
                    //hBaseApi.addOneSecondRowData(qualify,dataElementList,table);
                    //新版本Netty非阻塞存储方法  将数据封装成put格式的url进行请求
                    String rowKey = dataElementList.get(0).getDataCalendar();
                    String datalist = dataElementList.toString();
                    String httpPut = httpPut("http://localhost:45889/dqq/" + rowKey + "/" + "info:" + qualify, datalist, "UTF-8");
                    long endTime = System.currentTimeMillis();
                    long cost = endTime - currentTime;
                    System.out.println(qualify+" one second data saved,and spend time:"+cost);
                }
            }
            threadPoolExecutor.remove(this);
        } catch (IOException | ParseException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //httpClient put方法向HBase存储数据
    public static String httpPut(String urlPath,String data,String charSet){
        String res = null;
        URL url;
        HttpURLConnection httpURLConnection = null;
        try {
            url = new URL(urlPath);
            httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setConnectTimeout(2000000);
            httpURLConnection.setReadTimeout(2000000);
            httpURLConnection.setRequestProperty("Connection","close");


            httpURLConnection.setRequestMethod("PUT");
            httpURLConnection.setRequestProperty("Content-Type","text/plain");
            if(StringUtils.isNotBlank(data)){
                httpURLConnection.getOutputStream().write(data.getBytes(StandardCharsets.UTF_8));
            }
            httpURLConnection.getOutputStream().flush();
            httpURLConnection.getOutputStream().close();
            int code = httpURLConnection.getResponseCode();
            if(code == 200){
                DataInputStream in = new DataInputStream(httpURLConnection.getInputStream());
                int len = in.available();
                byte[] by = new byte[len];
                in.readFully(by);
                if(StringUtils.isNotBlank(charSet))
                    res = new String(by, Charset.forName(charSet));
                else
                    res = new String(by);
                in.close();
            }else {
                System.out.println("请求地址异常");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(httpURLConnection!=null)
                httpURLConnection.disconnect();
        }
        return res;
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
    public List<DataElement> readLocalDataOffLine() throws IOException {
        int by = -1;
        boolean fileIsOver = false;
        int loopCount = 0;
        List<DataElement> data = new ArrayList<>();
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
                            return new ArrayList<>();
                        }
                    }
                }
            }catch (IOException | InterruptedException e){
                e.printStackTrace();
                return new ArrayList<>();
            }
            loopCount++;
            byte[] feature = {dataByte[0],dataByte[1],dataByte[2],dataByte[3]};
            if (new String(feature).compareTo("HFME")==0){
                buffered.skip(this.datahead);
                buffered.read(dataByte);
            }
            DataElement dataElement = this.getDataElementFromDataBytes();
            dataElement.setDataCalendar(this.formerDate());
            data.add(dataElement);
            volt=Byte2OtherDataFormat.byte2Short(dataByte[this.voltstart],dataByte[this.voltend]);
            if (this.voltProcessing(volt,loopCount))
                break;
        }
        return data;
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
