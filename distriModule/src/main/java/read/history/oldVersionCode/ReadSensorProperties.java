package read.history.oldVersionCode;

import entity.ChannelInfo;
import read.utils.Byte2OtherDataFormat;
import read.utils.FindByte;

import java.io.*;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/22 上午10:38
 */
public class ReadSensorProperties {

    public ChannelInfo[] readSensor(File file) throws IOException {
        ChannelInfo[] sensor = new ChannelInfo[7];

        // 为上面的每个引用分配空间
        for (int i = 0; i < 7; i++) {
            sensor[i] = new ChannelInfo();
        }

        // 存放头文件字节数组
        byte[] fileHeadByte = new byte[186];

        // 存放通道信息
        byte[][] sensorArray = new byte[7][14];

        BufferedInputStream buffered = new BufferedInputStream(
                new FileInputStream(file));

        // 将读到的头文件放入到字符数组缓冲区中
        buffered.read(fileHeadByte);

        // 将读到的通道信息放到二维的数组中
        for (int i = 0; i < 7; i++) {
            buffered.read(sensorArray[i]);

            // 获取字节序列
            byte[] chNoByte = FindByte.searchByteSeq(sensorArray[i], 0, 1);
            byte[] chNameByte = FindByte.searchByteSeq(sensorArray[i], 2, 5);
            byte[] chUnitByte = FindByte.searchByteSeq(sensorArray[i], 6, 9);
            byte[] chCaliByte = FindByte.searchByteSeq(sensorArray[i], 10, 13);

            // 解析字符序列
            short chNo = Byte2OtherDataFormat.byte2Short(chNoByte);
            String chName = Byte2OtherDataFormat.byte2String(chNameByte);
            String chUnit = Byte2OtherDataFormat.byte2String(chUnitByte);
            byte[] tempFloat ={chCaliByte[3] ,chCaliByte[2] ,chCaliByte[1] ,chCaliByte[0]} ;
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(tempFloat));
            float f = dis.readFloat() ;
            dis.close();

            sensor[i].setChCali(f);
            sensor[i].setChName(chName);
            sensor[i].setChNo(chNo);
            sensor[i].setChUnit(chUnit);
        }

        buffered.close();

        return sensor;
    }

}
