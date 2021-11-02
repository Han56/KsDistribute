package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author han56
 * @description Hadoop客户端连接交互测试
 * @create 2021/9/14 上午9:23
 */
public class HdfsClient {

    private FileSystem fileSystem;

    /*
     * 初始化方法
     * */
    @Before
    public void init() throws URISyntaxException,IOException,InterruptedException{
        // 连接集群 nn 地址
        URI uri = new URI("hdfs://hadoop101:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","3");

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
     * 创建文件夹测试
     * */
    @Test
    public void testMkdir() throws IOException{
        //创建目录测试
        fileSystem.mkdirs(new Path("/test"));
    }



    /*
     * 配置文件优先级  hdfs-default.xml < hdfs-site.xml  < 客户端下 resource 文件夹下的 hdfs-site.xml < 代码中设置的优先级
     * */

    /*
     * 上传文件测试
     * */
    @Test
    public void testCopyFromLocalFile() throws IOException,URISyntaxException,InterruptedException{

        List<String> pathList = new ArrayList<String>(){{
            add("/home/han56/hadoop_input/alignRes/4AlgrithmAlginRes.txt");
            add("/home/han56/hadoop_input/alignRes/5AlgrithmAlginRes.txt");
            add("/home/han56/hadoop_input/alignRes/6AlgrithmAlginRes.txt");
        }};
        /*
         * 参数一：上传完成是否删除源文件  参数二：是否重写文件
         * 参数三：源文件路径  参数四：hdfs文件路径
         * */
        for (String pathStr:pathList)
            fileSystem.copyFromLocalFile(false,false,
                    new Path(pathStr),
                    new Path("hdfs://hadoop101:8020//hy_history_data/algin_group"));
        System.out.println("文件上传成功");
    }

    /*
     * 文件下载测试
     * */
    @Test
    public void testGet() throws IOException {
        /*
         * 参数一：源文件是否删除  参数二：源文件的路径
         * 参数三：目标地址路径    参数四：是否进行本地文件的校验
         * */
        fileSystem.copyToLocalFile(false, new Path("hdfs://hadoop102/xiyou/huaguoshan/hy.dxf"),
                new Path("/home/han56"), true);
    }


    /*
    * 读文件测试
    * */
    @Test
    public void testReadFile() throws IOException{
        Path readPath = new Path("hdfs://hadoop101:8020/hy_history_data/September/S/Test_190925110520.HFMED");

        //open file
        FSDataInputStream inputStream = fileSystem.open(readPath);

        try {
            IOUtils.copyBytes(inputStream,System.out,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //close Stream
            IOUtils.closeStream(inputStream);
        }
    }

    /*
     * 文件删除测试
     * */

    @Test
    public void testRm() throws IOException {
        /*
         * 参数一：待删除的路径， 参数二：是否递归删除
         * */
        fileSystem.delete(new Path("/jinguo"),true);
    }

    /*
     * 文件的更名与移动
     * */
    @Test
    public void testMv() throws IOException {
        /*
         * 参数一：源文件路径 参数二：目标文件的路径
         * */
        //对文件名称的修改
//        fileSystem.rename(new Path("/wcinput/word.txt"),
//                new Path("/wcinput/rename_word_test.txt"));
        //文件的移动和更名
//        fileSystem.rename(new Path("/wcinput/rename_word_test.txt"),
//                new Path("/cls.txt"));
        //对目录的更名
        fileSystem.rename(new Path("/test"),new Path("/rename_test"));
    }


    /*
     * 获取文件详细信息
     * */
    @Test
    public void getFileDetails() throws IOException {
        //获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/hy_history_data/September/S"),true);
        //遍历文件
        int sum=0;//计数器
        while (listFiles.hasNext()) {
            sum++;
            System.out.println("======输出第"+sum +"个文件信息======");
            LocatedFileStatus f = listFiles.next();
            //获取一些常规信息
            System.out.println(f.getPath());
            System.out.println(f.getAccessTime());
            System.out.println(f.getOwner());
            System.out.println(f.getPermission());
            //获取块信息
            BlockLocation[] blockLocations = f.getBlockLocations();
            for (BlockLocation flag:blockLocations)
                System.out.println(flag);
        }
    }
    /*
     * 判断是文件夹还是文件
     * */
    @Test
    public void testFile() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses){
            if (fileStatus.isFile())
                System.out.println(fileStatus.getPath().getName()+" is a file");
            else if (fileStatus.isDirectory())
                System.out.println(fileStatus.getPath().getName()+" is a dir");
            else
                System.out.println("未知");
        }
    }

}
