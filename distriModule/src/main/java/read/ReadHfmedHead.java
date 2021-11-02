package read;

import entity.HFMEDHead;
import entity.SensorProperties;
import read.utils.HDFSUtils;

import java.io.*;

/**
 * @author han56
 * @description 功能描述 【读取hfmed文件头部数据】
 * @create 2021/11/1 下午4:29
 */
public class ReadHfmedHead {

    public HDFSUtils utils;

    public HFMEDHead readHead(String fileName) throws IOException {

        return utils.readHFileHead(fileName);

    }


    /*
     * 功能：
     * this method read channel properties , but watch out , there should be 7
     * channels and 1 GPS channel , now the fact is that there only 6 channels
     * ahead and 1 GPS channel without the 7th channel.
     * @param fileName
     *            the file name
     * @return sensor array , sensor[0] ~ sensor[5] represents the sensor
     *         properties respectively from sensor 1 , to sensor 6 , and
     *         sensor[6] represents the GPS channel
     * @author Xingdong Yang. han56
     * @throws IOException
    * */

    public SensorProperties[] readSensorProperties(String fileName) throws IOException{
        return utils.readSensorProperties(fileName);
    }


}
