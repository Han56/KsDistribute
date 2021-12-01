package entity;

/**
 * @author han56
 * @description 功能描述
 * VO层实体类做 最终存储的数据整合
 * @create 2021/11/29 下午2:35
 */
public class VOEntityClass {

    private DataElement dataElement;

/*    private HfmedSegmentHead hfmedSegmentHead;

    private HFMEDHead hfmedHead;

    private ChannelInfo channelInfo;*/

    private String winStartDate;

    private String winEndDate;

    private String filePathName;

    private Integer dataGroupNum;

    public DataElement getDataElement() {
        return dataElement;
    }

    public void setDataElement(DataElement dataElement) {
        this.dataElement = dataElement;
    }

/*    public HfmedSegmentHead getHfmedSegmentHead() {
        return hfmedSegmentHead;
    }

    public void setHfmedSegmentHead(HfmedSegmentHead hfmedSegmentHead) {
        this.hfmedSegmentHead = hfmedSegmentHead;
    }

    public HFMEDHead getHfmedHead() {
        return hfmedHead;
    }

    public void setHfmedHead(HFMEDHead hfmedHead) {
        this.hfmedHead = hfmedHead;
    }

    public ChannelInfo getChannelInfo() {
        return channelInfo;
    }

    public void setChannelInfo(ChannelInfo channelInfo) {
        this.channelInfo = channelInfo;
    }*/

    public String getWinStartDate() {
        return winStartDate;
    }

    public void setWinStartDate(String winStartDate) {
        this.winStartDate = winStartDate;
    }

    public String getWinEndDate() {
        return winEndDate;
    }

    public void setWinEndDate(String winEndDate) {
        this.winEndDate = winEndDate;
    }

    public String getFilePathName() {
        return filePathName;
    }

    public void setFilePathName(String filePathName) {
        this.filePathName = filePathName;
    }

    public Integer getDataGroupNum() {
        return dataGroupNum;
    }

    public void setDataGroupNum(Integer dataGroupNum) {
        this.dataGroupNum = dataGroupNum;
    }
}
