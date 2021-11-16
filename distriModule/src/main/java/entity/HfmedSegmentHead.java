package entity;

import java.util.Arrays;

/**
 * @author hyena-yang
 * @description 功能描述
 * 数据段头
 * 注意：每一个数据段有一个数据段头，加下面的好几条数据
 *  * 解释如下：
 *  *  --------------------
 *  *     数据段头
 *  *  --------------------
 *  *      row1
 *  *      row2
 *  *      ...
 *  *      row3
 *  *      真正存放数据的地方
 *  *  --------------------
 * @create 2021/11/2 下午3:01
 */
public class HfmedSegmentHead {

    /** 特征码 */
    private String featureCode ;

    /** 数据段编号  */
    private int segmentNo ;

    /** 数据段头的时间 这个是有用的　*/
    private String sysTime ;

    /** usb传输前的系统微妙时钟   */
    private byte[] usbBeforeCurrency ;

    /** usb传输后的系统微秒时钟 */
    private byte[] usbAfterCurrency ;

    public HfmedSegmentHead() {
        super();
        // TODO Auto-generated constructor stub
    }

    public String getFeatureCode() {
        return featureCode;
    }

    public void setFeatureCode(String featureCode) {
        this.featureCode = featureCode;
    }

    public int getSegmentNo() {
        return segmentNo;
    }

    public void setSegmentNo(int segmentNo) {
        this.segmentNo = segmentNo;
    }

    public String getSysTime() {
        return sysTime;
    }

    public void setSysTime(String sysTime) {
        this.sysTime = sysTime;
    }
}
