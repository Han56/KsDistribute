package read.utils;

/**
 * @author han56
 * @description 功能描述
 * 将byte类型转换为不同的数据类型
 * @create 2021/11/2 上午9:46
 */
public class Byte2OtherDataFormat {

    public static int byte2Int(byte[] temp){
        int a = ( temp[3] & 0xff) <<24 ;

        int b = (temp[2] & 0xff) << 16 ;

        int c = (temp[1] & 0xff) << 8 ;

        int d = (temp[0] & 0xff) ;

        return a + b + c + d ;
    }

    /**
     *
     * @param low  the short of low 8 bits
     * @param high the short of high 8 bits
     * @return
     */
    public static short byte2Short(byte low , byte high ){

        return  (short) ( high <<8 | low & 0xff)  ;

    }

    public static short byte2Short(byte[] shortByte){
        if( shortByte.length > 2){
            throw new ArrayIndexOutOfBoundsException() ;
        }

        byte low = shortByte[0]  ;

        byte high = shortByte[1] ;

        return byte2Short(low , high) ;
    }

    /**
     * @param stringByte  the byte to be resolved to String
     * @return
     */
    public static String byte2String(byte[] stringByte){

        return new String(stringByte) ;
    }

}
