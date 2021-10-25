package hdfsTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author han56
 * @description 功能描述 【各功能测试模块】
 * @create 2021/10/23 上午9:59
 */
public class Test {

    private static FileSystem fileSystem;

    /*
    * 预定义文件夹
    * */
    //预先定义所有文件夹
/*    List<String> fileParentPath = new ArrayList<String>(){{
        add("/hy_history_data/September/S");
        add("/hy_history_data/September/T");
        add("/hy_history_data/September/U");
        add("/hy_history_data/September/V");
        add("/hy_history_data/September/Y");
        add("/hy_history_data/September/Z");
    }};*/

    //对齐结果集
    List<List<String>> backTrackRes = new LinkedList<>();

    @Before
    public void init() throws URISyntaxException, IOException,InterruptedException{
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

    String rootTest="/hy_history_data/September";
    @org.junit.Test
    public void sortTest() throws IOException {
        System.out.println(sortShortestFilePath(rootTest));
    }

    /*
    * 按照文件数量 排序，排列文件路径
    * */
    public static List<String> sortShortestFilePath(String rootPath) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(rootPath));
        List<String> res = new LinkedList<>();
        final Map<String,Integer> panfuToNumMap = new HashMap<>();
        for (FileStatus fileStatus:fileStatuses){
            if (fileStatus.isFile()){
                System.out.println(fileStatus.getPath().getName()+"是一个文件");
            }else if (fileStatus.isDirectory()){
//                System.out.println(fileStatus.getPath().getName()+"是一个文件夹");
                String dirName = fileStatus.getPath().getName();
                //将其进行拼接
                //统计当前文件夹下文件个数
                int fileNum = 0;
                RemoteIterator<LocatedFileStatus> listsFile = fileSystem.listFiles(
                        new Path(rootPath + "/" + dirName
                                //统计当前文件夹下文件个数
                        ),true);
                while (listsFile.hasNext()){
                    fileNum++;
                    listsFile.next();
                }
                panfuToNumMap.put(dirName,fileNum);
            }else {
                System.out.println("既不是文件也不是文件夹");
            }
        }
        //转换为list
        List<Map.Entry<String,Integer>> list = new ArrayList<>(panfuToNumMap.entrySet());
        //使用Collections.sort()排序，按照value排序
        list.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                //降序 o2 conpareTo o1  升序 o1 compareTo o2
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        //循环输出测试
//        System.out.println("根据value升序排序输出测试");
        for (Map.Entry<String,Integer> mapResults:list){
//            System.out.println(mapResults.getKey()+":"+mapResults.getValue());
            res.add(rootPath+"/"+mapResults.getKey());
        }
        return res;
    }


    /*
    * 对齐文件测试方法
    * */
    @org.junit.Test
    public void formatFilePath() throws IOException, ParseException {
        //结果集
        Map<Character,List<String>> returnMap = new HashMap<>();
        //存储盘符
        List<Character> panfus = new ArrayList<>();
        /*
        * 优化后：文件夹列表按照文件数大小升序排序
        * */
        List<String> fileParentPath = sortShortestFilePath("/hy_history_data/September");
        //设置排序阈值

        //获取所有文件信息
        for (String pathStr:fileParentPath){
//            System.out.println("=====当前盘符："+pathStr.charAt(pathStr.length()-1)+"=====");
            RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles
                    (new Path(pathStr),true);
            //遍历文件
            int sum = 0;
            List<String> resultList = new ArrayList<>();
            while (listFiles.hasNext()){
                sum++;
//                System.out.println("======输出第"+ sum +"个文件信息======");
                LocatedFileStatus f = listFiles.next();
//                System.out.println("======文件路径:"+f.getPath()+"====");
                resultList.add("20"+f.getPath().toString().substring(55,67));
            }
            char panfu = pathStr.charAt(pathStr.length()-1);
            panfus.add(panfu);
            returnMap.put(panfu,resultList);
        }
        /*
        * 输出测试
        * */
/*        for (char panfu:panfus){
            System.out.println(returnMap.get(panfu));
        }*/
        //进入回溯算法处理
        LinkedList<String> track = new LinkedList<>();
        backtrack(track,returnMap,panfus,0,3);
        //回溯完成 res 结果集存储对齐初步结果
        System.out.println("=====根据文件名对齐结果=====");
        for (List<String> printList:backTrackRes)
            System.out.println(printList);
    }

    /*
    * 回溯递归方法
    * */
    public void backtrack(LinkedList<String> track,Map<Character,List<String>> map,List<Character> panfus,int start,int k) throws ParseException {
        if (track.size() == k){
            backTrackRes.add(new LinkedList<>(track));
            return;
        }
        List<String> tmpList = map.get(panfus.get(start));
        for (String date : tmpList) {
            /*
            * 用当前的日期的日期与track中其他日期对比，
            * 查看是否在同一个小时内
            * */
            if (!isValid(track, date))
                continue;
            track.add(date);
            backtrack(track, map, panfus, start + 1,k);
            track.removeLast();
        }
    }



    /*
    * 回溯算法日期判断方法
    * 判断规则：@param dateBeforeJudge 与 @param track 中的所有元素进行比较
    *         在一小时内的话则返回true，反之返回false
    * */
    LinkedList<String> testTrack = new LinkedList<String>(){{
       add("20190925110520");
       add("20190925114506");
       add("20190925115915");
       add("20190925110949");
       add("20190925111650");
    }};
    String testDate = "20190925113745";

    @org.junit.Test
    public void testIsValid() throws ParseException {
        System.out.println(isValid(testTrack,testDate));
    }



    /*
    * 用于回溯
    * */
    public static boolean isValid(LinkedList<String> track,String dateBeforeJudge) throws ParseException {
        for (String trackStr:track){
            //对数据进行切割，先看是否在同一天
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            Date trackStrDate = simpleDateFormat.parse(trackStr.substring(0,8));
            Date dateBeforeJudgeDate = simpleDateFormat.parse(dateBeforeJudge.substring(0,8));
            if (!trackStrDate.equals(dateBeforeJudgeDate)){
                //System.out.println("不再同一天");
                return false;
            }
            //如果是同一天，则比较时间差是否在一个小时以内
            /*
            * 这里之后会封装成一个计算秒级别差值的方法
            * 这里这样写一定会加快速度，毕竟很多时间都执行不到这一步
            * 避免每次都要计算时间差
            * */
            SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss");
            Date trackStrDate1 = simpleDateFormat1.parse(trackStr);
            Date dateBeforeJudgeDate1 =  simpleDateFormat1.parse(dateBeforeJudge);
            //时间差
            int diff = Math.abs((int)((trackStrDate1.getTime()-dateBeforeJudgeDate1.getTime())/1000));
            //System.out.println("时间差:"+diff);
            if (Math.abs(diff)>3600){
                //System.out.println("在同一天，但是时间差大于一个小时");
                return false;
            }
        }
        //System.out.println("成功");
        return true;
    }


    public static boolean isOverTrackElement(LinkedList<String> track,String dateBeforeJudge) throws ParseException {
        for (String trackStr:track){
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            Date trackStrDate = simpleDateFormat.parse(trackStr.substring(0,8));
            Date dateBeforeJudgeDate = simpleDateFormat.parse(dateBeforeJudge.substring(0,8));
            if (dateBeforeJudgeDate.after(trackStrDate)){
                return false;
            }
        }
        return true;
    }
}
