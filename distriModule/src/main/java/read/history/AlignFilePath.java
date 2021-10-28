package read.history;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author han56
 * @description 功能描述 【对齐操作】
 * @create 2021/10/25 下午3:57
 */
public class AlignFilePath {

    private static FileSystem fileSystem;

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
    * 对齐方法
    * */
    @Test
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
            List<String> resultList = new ArrayList<>();
            while (listFiles.hasNext()){
                LocatedFileStatus f = listFiles.next();
//                System.out.println("======文件路径:"+f.getPath().toString()+"====");
                resultList.add(f.getPath().toString());
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
        backtrack(track,returnMap,panfus,0,6);
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
             * 对date进行切割组装
             * */
            String subDate = "20"+date.substring(55,67);
            /*
             * 用当前的日期的日期与track中其他日期对比，
             * 查看是否在同一个小时内
             * */
            if (!isValid(track, subDate))
                continue;
            track.add(date);
            backtrack(track, map, panfus, start + 1,k);
            track.removeLast();
        }
    }

    /*
     * 用于回溯的判断
     * */
    public static boolean isValid(LinkedList<String> track,String dateBeforeJudge) throws ParseException {
        for (String trackStr:track){
            //对数据进行切割，先看是否在同一天  /hy_history_data/September/S/Test_1909223080744.HFMED
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            Date trackStrDate = simpleDateFormat.parse("20"+trackStr.substring(55,61));
            Date dateBeforeJudgeDate = simpleDateFormat.parse(dateBeforeJudge.substring(0,8));
            if (!trackStrDate.equals(dateBeforeJudgeDate)){
                return false;
            }
            //如果是同一天，则比较时间差是否在一个小时以内
            /*
             * 这里之后会封装成一个计算秒级别差值的方法
             * 这里这样写一定会加快速度，毕竟很多时间都执行不到这一步
             * 避免每次都要计算时间差
             * */
            SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyyMMddHHmmss");
            Date trackStrDate1 = simpleDateFormat1.parse("20"+trackStr.substring(55,67));
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



}
