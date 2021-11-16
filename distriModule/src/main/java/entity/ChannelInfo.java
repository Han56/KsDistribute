package entity;

/**
 * @author han56
 * @description 功能描述
 * 通道信息实体类
 * @create 2021/11/14 下午3:23
 */
public class ChannelInfo {

    //编号表示开通的通道
    private int chNo;

    //通道采集名称（速度，电位，电磁）
    private String chName;

    //通道数据单位
    private String chUnit;

    //通道标定数据
    private float chCali;

    public int getChNo() {
        return chNo;
    }

    public void setChNo(int chNo) {
        this.chNo = chNo;
    }

    public String getChName() {
        return chName;
    }

    public void setChName(String chName) {
        this.chName = chName;
    }

    public String getChUnit() {
        return chUnit;
    }

    public void setChUnit(String chUnit) {
        this.chUnit = chUnit;
    }

    public float getChCali() {
        return chCali;
    }

    public void setChCali(float chCali) {
        this.chCali = chCali;
    }

    /*
     * 重载 toString 方法
     * */
    @Override
    public String toString() {
        return "SensorProperties [chNo=" + chNo + ", chName=" + chName
                + ", chUnit=" + chUnit + ", chCali=" + chCali + "]";
    }
}
