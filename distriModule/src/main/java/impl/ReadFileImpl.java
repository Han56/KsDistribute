package impl;

import entity.ChannelInfo;
import entity.HFMEDHead;
import entity.HfmedSegmentHead;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

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
    HfmedSegmentHead getDataHeadInfoByFile(FSDataInputStream fsDataInputStream,int segmentNum,short channelOnNum) throws IOException;

    //读取数据段


}
