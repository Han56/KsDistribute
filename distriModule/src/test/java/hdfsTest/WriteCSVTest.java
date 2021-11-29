package hdfsTest;

import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @author han56
 * @description 功能描述
 * 写入CSV存储测试
 * @create 2021/11/29 下午2:15
 */
public class WriteCSVTest {

    @Test
    public void testWrite(){

        /*
        * 封装测试数据
        * */


        FileOutputStream outputStream = null;
        OutputStreamWriter outputStreamWriter = null;
        BufferedWriter bufferedWriter = null;
        try {
            File finalCSVFile = new File("/data/files/DownLoads/test.csv");
            outputStream = new FileOutputStream(finalCSVFile);
            outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
            //s手动添加BOM标识，防止乱码
            outputStreamWriter.write(new String(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF }));
            bufferedWriter = new BufferedWriter(outputStreamWriter);
            /*
            * 往CSV文件中存储数据
            * */
            String title = "";
            title = "姓名，性别，年龄，手机号，地址";
            bufferedWriter.append(title).append("\r");


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
