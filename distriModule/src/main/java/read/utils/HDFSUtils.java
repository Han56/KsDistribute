package read.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author han56
 * @description 功能描述 【HDFS】相关客户端操作
 * @create 2021/10/19 上午11:02
 */
public class HDFSUtils {

    private static FileSystem fileSystem;
    //预先定义所有文件夹
    List<String> fileParentPath = new ArrayList<String>(){{
        add("/hy_history_data/September/S");
        add("/hy_history_data/September/T");
        add("/hy_history_data/September/U");
        add("/hy_history_data/September/V");
        add("/hy_history_data/September/Y");
        add("/hy_history_data/September/Z");
    }};

    /*
    * hdfs客户端连接初始化操作
    * */
    public static void hdfsInit() throws IOException, InterruptedException, URISyntaxException {
        //连接集群nameNode地址
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
    * hdfs结束操作
    * */
    public static void hdfsClose() throws IOException{
        fileSystem.close();
    }

    /*
    * 获取每个文件夹的文件名，并进行封装
    * */
    public Map<Character, List<String>> formatFilePathSet() throws IOException, URISyntaxException, InterruptedException {

        hdfsInit();

        //结果集
        Map<Character,List<String>> returnMap = new HashMap<>();

        //获取所有文件信息
        for (String pathStr:fileParentPath){
            System.out.println("=====当前盘符："+pathStr.charAt(pathStr.length()-1)+"=====");
            RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles
                    (new Path(pathStr),true);
            //遍历文件
            int sum = 0;
            List<String> resultList = new ArrayList<>();
            while (listFiles.hasNext()){
                sum++;
                System.out.println("======输出第"+sum +"个文件信息======");
                LocatedFileStatus f = listFiles.next();
                System.out.println("======文件路径:"+f.getPath()+"====");
                resultList.add(f.getPath().toString());
            }
            returnMap.put(pathStr.charAt(pathStr.length()-1),resultList);
        }
        hdfsClose();
        return returnMap;
    }

    /*
    * 获取所有文件夹下最晚开始的文件名称
    * */
    public List<Map<Character,String>> getTheLastestFilePath() throws IOException, URISyntaxException, InterruptedException {
        hdfsInit();

        List<Map<Character,String>> lists = new ArrayList<>();
        //获取各个文件夹下的第一个文件
        for (String pathStr:fileParentPath){
            System.out.println("=====当前盘符："+pathStr.charAt(pathStr.length()-1)+"=====");
            RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(
                    new Path(pathStr),true);
            Map<Character,String> listMap = new HashMap<>();
            System.out.println("the lastet filePath:"+listFiles.next().getPath().toString());
            listMap.put(pathStr.charAt(pathStr.length()-1)
                    ,listFiles.next().getPath().toString());
            lists.add(listMap);
        }

        hdfsClose();
        return lists;
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        HDFSUtils utils = new HDFSUtils();
        /*
        * 遍历所有文件测试
        * */
        /*Map<Character,List<String>> testMap = utils.formatFilePathSet();
        List<String> testList = testMap.get('S');
        for (String testStr:testList)
            System.out.println(testStr);*/

        /*
        * 【测试】获取文件夹下第一个文件
        * */
        List<Map<Character,String>> testLists = utils.getTheLastestFilePath();
        System.out.println("测试结果集：");
        System.out.println(testLists);
    }


}
