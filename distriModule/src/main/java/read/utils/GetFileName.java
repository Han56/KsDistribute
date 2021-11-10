package read.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author han56
 * @description 功能描述：获取目录下符合日期约束的文件名
 * @create 2021/9/23 下午7:40
 */
public class GetFileName {

    public static void main(String[] args) {
        File f = new File("/run/media/han56/新加卷/红阳三矿/201909/S");
        //列出该文件夹下的所有文件
/*        File[] files = f.listFiles();
          printFilesName(files);*/
        //过滤剩下后缀名为 .HFMED 的文件
        File[] filteredFiles = f.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".HFMED");
            }
        });
        System.out.println("过滤操作后的文件:");
        printFilesName(filteredFiles);
    }

    /*
     * @param:文件父目录路径 parentPath
     * */
    public List<String> filePathList(String parentPath){
        File f = new File(parentPath);
        List<String> returnList = new ArrayList<>();
        //过滤出后缀名为 .HFMED 的文件
        File[] filteredFiles = f.listFiles(new FilenameFilter(){

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".HFMED");
            }
        });
        if (filteredFiles!=null){
            for (File file:filteredFiles)
                returnList.add(file.getAbsolutePath());
        }
        return returnList;
    }

    //print the files name
    public static void printFilesName(File[] files){
        System.out.println("======");
        if (files!=null){
            for (File file:files)
                System.out.println(file.getAbsolutePath());
        }
        System.out.println("======");
    }

}
