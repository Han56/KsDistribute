package impl;

import entity.*;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.List;

/**
 * @author han56
 * @description 功能描述
 * 读取文件接口（操作方法集合）
 * @create 2021/11/14 下午3:20
 */
public interface ReadFileImpl {

    //读取文件头（每个文件第一段）
    HFMEDHead readFileHead(FSDataInputStream fsDataInputStream) throws IOException;

    //读取通道信息（本段位于文件头之后，并根据ChannelOnNum重复Num次）
    ChannelInfo[] getChannelInfoByFile(FSDataInputStream fsDataInputStream,short channelOnNum) throws IOException;

    //读取数据段头
    HfmedSegmentHead getDataHeadInfoByFile(FSDataInputStream fsDataInputStream,byte[] preRead,short channelOnNum) throws IOException;

    //读取数据段
    /*
    * 参数1：fsDataInputStream 输入流
    * 参数2：通道数
    * */
    DataElement getDataInfoByFile(FSDataInputStream fsDataInputStream,byte[] preRead,short channelOnNum,int loopCount) throws IOException;

    //存储成csv接口
    void saveCSV(List<VOEntityClass> list,String savePath);

    //存储txt文件接口
    void saveTxt(List<VOEntityClass> list,String saveDirPath,String saveFileName) throws IOException;

}
