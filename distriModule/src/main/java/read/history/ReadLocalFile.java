package read.history;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import read.utils.DateUtils;
import read.utils.ReadFileThread;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/22 上午9:35
 */
public class ReadLocalFile{

    //HDFS定义
    private static FileSystem fileSystem;


    public static volatile int count = 0;

    /*
    * HBase初始化
    * */
    public static void init() throws URISyntaxException,IOException,InterruptedException{
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

    /*
    * HBase关闭操作
    * */
    public static void close() throws IOException{
        fileSystem.close();
    }

    /*
    * 设置全部参数集合
    * */
    public static List<List<String>> setThreadParams() throws IOException, ParseException, InterruptedException, URISyntaxException {

        /*
         * 读取对齐分组文件中一行数据，分割空格将其塞进Map<Integer,List<String>>容器中
         * */
        init();
        BufferedReader reader = new BufferedReader(new InputStreamReader
                (fileSystem.open(new Path("hdfs://hadoop101:8020/hy_history_data/algin_group/6AlgrithmAlginRes.txt")))
        );
        HashMap<Integer,List<String>> map = new HashMap<>();
        String line;
        int lineNum = 1;
        while ((line = reader.readLine())!=null){
            List<String> groupFilePath = new ArrayList<>();
            String[] s = line.split(" ");
            Collections.addAll(groupFilePath, s);
            map.put(lineNum,groupFilePath);
            lineNum++;
        }
        reader.close();

        List<List<String>> res = new ArrayList<>();

        for (int i=1;i <= map.size() ;i++){
            List<String> filesPath;
            filesPath = map.get(i);
            DateUtils dateUtils = new DateUtils();
            List<String> startAndEndTime = dateUtils.getStartAndEndTime(filesPath);
            String winStart = startAndEndTime.get(0);String winEnd = startAndEndTime.get(1);


            for (String oneFile:filesPath){
                List<String> threadParams = new ArrayList<>();
                //获取列名
                String qualify = oneFile.substring(0,1);
                /*
                * 文件前缀，完整路径
                * */
                String prePathStr = "I:\\红阳三矿\\201909\\";
                String totalFileStr = prePathStr+oneFile.replace('/','\\');
                threadParams.add(qualify);
                threadParams.add(totalFileStr);
                threadParams.add(winStart);
                threadParams.add(winEnd);
                res.add(threadParams);
            }
        }
        close();
        return res;
    }

    /*
    * HBase配置部分
    * */
    public static Configuration configuration;
    public static Connection connection;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException, URISyntaxException {
        Table table = connection.getTable(TableName.valueOf("dqq"));
        //定义线程池
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                10,20,60*3, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(50), Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
        Object lock = new Object();

        //获取参数
        List<List<String>> threadPoolParams = setThreadParams();

        //设置连接对象 List<String> list:threadPoolParams
        for(int i=0;i<threadPoolParams.size();i++){
            ReadFileThread readFileThread1 = new ReadFileThread(threadPoolParams.get(i).get(1),
                    threadPoolParams.get(i).get(0),threadPoolParams.get(i).get(2),threadPoolParams.get(i).get(3),table,lock,threadPoolExecutor);
            //System.out.println("Thread Pool is Created Successfully!");
            threadPoolExecutor.execute(readFileThread1);
            System.out.println("Thread-start-"+threadPoolParams.get(i).get(0));
            synchronized (lock){
                count++;
            }
            if(count == 10){
                System.out.println("当前核心线程数已满，暂停循环");
                synchronized (lock){
                    lock.wait();
                }
            }
        }
        table.close();

    }

}
