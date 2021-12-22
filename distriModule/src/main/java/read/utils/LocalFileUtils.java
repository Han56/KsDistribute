package read.utils;

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * @author han56
 * @description 功能描述【本地文件操作工具类】
 * @create 2021/10/30 上午9:56
 */
public class LocalFileUtils {

    @Test
    public void fileOutDemo() throws IOException {
        File file = new File("test1.txt");
        //create file
        file.createNewFile();
        //create a FileWriter Object
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write("test");
        fileWriter.write("\n");
        fileWriter.write("test2");
        fileWriter.flush();
        fileWriter.close();
    }

    public void alignResToFileToSave(List<List<String>> res,int k) throws IOException{

        File file = new File(k+"AlgrithmAlginRes.txt");

        //创建文件
        FileWriter fileWriter = new FileWriter(file);

        for (List<String> dataGroup:res){
            for (String path:dataGroup){
                fileWriter.write(path);
                fileWriter.write(" ");
            }
            fileWriter.write("\n");
        }
        System.out.println(k+"路径对齐结果存储完成");
        fileWriter.flush();
        fileWriter.close();
    }

}
