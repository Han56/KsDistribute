package hdfsTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import read.utils.LocalFileUtils;

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


    //对齐结果集
    List<List<String>> res = new LinkedList<>();

    /*
     * 列表index结果集
     * */
    List<List<Integer>> pathIndexRes = new LinkedList<>();

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
        /*
        * 优化后：文件夹列表按照文件数大小升序排序
        * */
        List<String> fileParentPath = sortShortestFilePath("/hy_history_data/September");

        List<List<String>> filePathSet = new ArrayList<>();

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
            filePathSet.add(resultList);
        }

        /*
        * 第一次回溯：将路径index进行组合
        * */
        Test test = new Test();
        List<List<Integer>> combinIndexList = test.combineListIndex(filePathSet.size(),5);
        System.out.println("====所有路径索引集合====");
        for (List<Integer> integers:combinIndexList)
            System.out.println(integers);
        System.out.println("====完毕====");
        //进入第二次回溯算法处理
        LinkedList<String> track = new LinkedList<>();
        for (List<Integer> path:combinIndexList)
            backtrack(path,track,filePathSet,0);
        System.out.println("===对齐结果===");
        for (List<String> testOut:res)
            System.out.println(testOut);
        System.out.println("===完毕===");
        System.out.println("====结果一共"+res.size()+"个====");
        System.out.println("====对结果进行存储====");
        LocalFileUtils localFileUtils = new LocalFileUtils();
        localFileUtils.alignResToFileToSave(res,5);
    }

    /*
     * 对list index进行回溯组合
     * */
    public List<List<Integer>> combineListIndex(int n,int k){
        if (k<3||n<0)
            return pathIndexRes;
        LinkedList<Integer> track = new LinkedList<>();
        pathIndexBackTrack(n,k,0,track);
        return pathIndexRes;
    }

    public void pathIndexBackTrack(int n,int k,int start,LinkedList<Integer> track){
        if (k == track.size()){
            pathIndexRes.add(new LinkedList<>(track));
            return;
        }
        for (int i=start;i<n;i++){
            track.add(i);
            pathIndexBackTrack(n, k, i+1, track);
            track.removeLast();
        }
    }

    /*
    * 第二次回溯递归方法
    * */
    public void backtrack(List<Integer> pathIndexList,LinkedList<String> track,List<List<String>> allPathList,int start) throws ParseException {
        if (track.size()==pathIndexList.size()){
            res.add(new LinkedList<>(track));
            return;
        }
        if (start==pathIndexList.size())
            return;
        int index=pathIndexList.get(start);
        List<String> temp= allPathList.get(index);
        Collections.sort(temp);
        for (String i:temp){
            if (!isOverTrackElement(track,i))
                return;
            if (!isValid(track,i))
                continue;
            track.add(i);
            backtrack(pathIndexList, track, allPathList, start+1);
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
        if (track.contains(dateBeforeJudge))
            return false;
        if (dateBeforeJudge==null)
            return false;
        for (String trackStr:track){
            //对数据进行切割，先看是否在同一天  /hy_history_data/September/S/Test_1909223080744.HFMED
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            Date trackStrDate = simpleDateFormat.parse("20"+trackStr.substring(55,61));
            Date dateBeforeJudgeDate = simpleDateFormat.parse("20"+dateBeforeJudge.substring(55,61));
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
            Date dateBeforeJudgeDate1 =  simpleDateFormat1.parse("20"+dateBeforeJudge.substring(55,67));
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
            Date trackStrDate = simpleDateFormat.parse("20"+trackStr.substring(55,61));
            Date dateBeforeJudgeDate = simpleDateFormat.parse("20"+dateBeforeJudge.substring(55,61));
            if (dateBeforeJudgeDate.after(trackStrDate)){
                return false;
            }
        }
        return true;
    }
}
