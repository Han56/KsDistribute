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

    @Test
    public void readLocal() throws IOException, ParseException, InterruptedException {

        /*
         * 读取对齐分组文件中一行数据，分割空格将其塞进Map<Integer,List<String>>容器中
         * */
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

        for (int i=1;i <= 1;i++){
            List<String> filesPath;
            filesPath = map.get(i);
            DateUtils dateUtils = new DateUtils();
            List<String> startAndEndTime = dateUtils.getStartAndEndTime(filesPath);
            String winStart = startAndEndTime.get(0);String winEnd = startAndEndTime.get(1);

            List<ReadFileThread> readFileThreads = new ArrayList<>();
            //改为异步读取
            for (String oneFile:filesPath){
                //获取列名
                String qualify = oneFile.substring(0,1);
                /*
                * 文件前缀，完整路径
                * */
                String prePathStr = "/data/files/DownLoads/KsDisIn/";
                String totalFileStr = prePathStr+oneFile;
                //将以上参数 通过Runnable构造函数进行初始化
            }
            //线程工作初始化完成 开启线程池
//            for (ReadFileThread readFileThread:readFileThreads){
//                Thread thread = new Thread(readFileThread);
//                thread.start();
//            }

        }
    }

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

    public static void main(String[] args) throws IOException {
        //定义日志
        //Logger logger = LoggerFactory.getLogger(ReadLocalFile.class);
        Table table = connection.getTable(TableName.valueOf("dq"));
        ReadFileThread readFileThread1 = new ReadFileThread("/data/files/DownLoads/KsDisIn/S/Test_190925110520.HFMED",
                "S","2019-09-25 11:05:20","2019-09-25 11:09:49",table);
        ReadFileThread readFileThread2 = new ReadFileThread("/data/files/DownLoads/KsDisIn/U/Test_190925105915.HFMED",
                "U","2019-09-25 11:05:20","2019-09-25 11:09:49",table);
        ReadFileThread readFileThread3 = new ReadFileThread("/data/files/DownLoads/KsDisIn/Y/Test_190925101650.HFMED",
                "Y","2019-09-25 11:05:20","2019-09-25 11:09:49",table);
        ReadFileThread readFileThread4 = new ReadFileThread("/data/files/DownLoads/KsDisIn/Z/Test_190925103745.HFMED",
                "Z","2019-09-25 11:05:20","2019-09-25 11:09:49",table);
        ReadFileThread readFileThread5 = new ReadFileThread("/data/files/DownLoads/KsDisIn/V/Test_190925100949.HFMED",
                "V","2019-09-25 11:05:20","2019-09-25 11:09:49",table);
        ReadFileThread readFileThread6 = new ReadFileThread("/data/files/DownLoads/KsDisIn/T/Test_190925104506.HFMED",
                "T","2019-09-25 11:05:20","2019-09-25 11:09:49",table);

        //Thread thread1 = new Thread(readFileThread1);
//        Thread thread2 = new Thread(readFileThread2);
//        thread1.start();
//        thread2.start();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                6,20,60*3, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100), Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
        System.out.println("Thread Pool is Created Successfully!");
        threadPoolExecutor.execute(readFileThread1);
        System.out.println("Thread1-start-S");
        threadPoolExecutor.execute(readFileThread2);
        System.out.println("Thread2-start-U");
        threadPoolExecutor.execute(readFileThread3);
        System.out.println("Thread3-start-Y");
        threadPoolExecutor.execute(readFileThread4);
        System.out.println("Thread4-start-Z");
        threadPoolExecutor.execute(readFileThread5);
        System.out.println("Thread5-start-V");
        threadPoolExecutor.execute(readFileThread6);
        System.out.println("Thread6-start-T");

        table.close();

    }

}
