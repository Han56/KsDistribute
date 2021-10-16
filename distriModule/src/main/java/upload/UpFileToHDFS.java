package upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import read.GetFileName;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 *@description 功能描述【上传】将本地红阳9月文件上传到集群
 *@author han56
 *@create 2021/10/7 下午6:25
 */public class UpFileToHDFS {

    private FileSystem fileSystem;


    /*
     * 初始化方法
     * */
    public void init() throws IOException, InterruptedException, URISyntaxException {
        //Link to 集群 nn 地址
        URI uri =  new URI("hdfs://hadoop101:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","1");

        //用户名
        String usserName = "han56";
        fileSystem = FileSystem.get(uri,configuration,usserName);
    }

    /*
     * 操作完成后关闭文件系统方法
     * */
    public void closeFileSys() throws IOException {
        fileSystem.close();
    }

    /*
     * 创建文件夹方法
     * */
    public void mkDir(List<String> pathStr) throws IOException {
        for (String str:pathStr)
            fileSystem.mkdirs(new Path(str));
        System.out.println("创建文件夹成功");
    }

    /*
     * 根据目录上传文件方法
     * */
    public void uploadFileByAbsolutePath() throws IOException {

        GetFileName getFileName = new GetFileName();
        String[]  diskName = {"Z"};
        for (String s : diskName) {
            List<String> fileAbsolutePathList = getFileName.filePathList("/run/media/han56/新加卷/红阳三矿/201909/" + s);
            int flag = 0;
            System.out.println("====正在上传"+s+"盘符的数据====");
            System.out.println("该盘符中共有"+fileAbsolutePathList.size()+"个.HMED后缀文件");
            for (String path : fileAbsolutePathList) {
                fileSystem.copyFromLocalFile(false, false,
                        new Path(path), new Path("hdfs://hadoop101/hy_history_data/September/" + s));
                flag++;
                System.out.println("第"+flag+"个文件:"+path + "  had uploaded to hdfs finished!");
            }
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        UpFileToHDFS upFileToHDFS = new UpFileToHDFS();
        //初始化
        upFileToHDFS.init();

        //创建文件夹
/*        List<String> path = new ArrayList<>();
        path.add("/hy_history_data/September/S");path.add("/hy_history_data/September/U");
        path.add("/hy_history_data/September/T");path.add("/hy_history_data/September/V");
        path.add("/hy_history_data/September/Y");path.add("/hy_history_data/September/Z");
        upFileToHDFS.mkDir(path);*/

        /*
         * 上传文件操作
         * */
        upFileToHDFS.uploadFileByAbsolutePath();

        //操作完成，关闭文件系统
        upFileToHDFS.closeFileSys();
    }

}