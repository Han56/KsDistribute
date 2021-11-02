package read;

import entity.HFMEDHead;
import read.utils.HDFSUtils;

import java.io.*;

/**
 * @author han56
 * @description 功能描述 【读取hfmed文件头部数据】
 * @create 2021/11/1 下午4:29
 */
public class ReadHfmedHead {

    public HFMEDHead readHead(String fileName) throws IOException {

        HDFSUtils utils = new HDFSUtils();

        return utils.readHFileHead(fileName);

    }





}
