package entity;

/**
 * @author han56
 * @description 功能描述【HFMED文件头部数据实体类】
 * @create 2021/11/1 下午4:15
 */
public class HFMEDHead {

    //文件头长度
    private short fileHeadLength;

    //数据格式版本号
    private String formatVer;

    //数据文件名称
    private String dataFileName;

    //操作员
    private String operator;

    //观测地点名称
    private String palaceName;

    //系统启动时间
    private byte[] sysStartTime;

    /*
    * 系统微秒计数器
    * Java Long占八个字节
    * */
    private long sysCounter;

    //系统运行频率
    private long sysFrequency;

    //用户指定的文件特定名称
    private String userIdName;

    //模数转换频率
    private Integer adFre;

    //ad分辨率
    private short resolution;

    //文件记录时间长度，使用浮点型 4个字节
    private Float fileDuration;

    //数据文件最大数据段数
    private Integer segmentNum;

    //段头长度字节数
    private short segmentHeadLength;

    //索引文件段头长度
    private short indexSegmentHeadLength;

    /** 个采样段所包含的记录条数 */
    private int segmentRecNum ;

    /** 段记录时间长度 */
    private float segmentDuration ;

    /** 特征码 。将在后面的每一个额数据段里面，出现在断头的最后4个字节
     * 这里取为 ：HFME（大写）*/
    private String featureCode ;

    /** 本单元所使用的通道数 1 - 7 */
    private short channelOnNum ;

    /** 文件编号，从1开始
     *  文档上的另一种解释：保留4个字节，留作微调
     * */
    private int reserve ;


    /*
    * 构造方法
    * */
    public HFMEDHead(short fileHeadLength, String formatVer, String dataFileName, String operator, String palaceName, byte[] sysStartTime, long sysCounter, long sysFrequency, String userIdName, Integer adFre, short resolution, Float fileDuration, Integer segmentNum, short segmentHeadLength, short indexSegmentHeadLength, int segmentRecNum, float segmentDuration, String featureCode, short channelOnNum, int reserve) {
        this.fileHeadLength = fileHeadLength;
        this.formatVer = formatVer;
        this.dataFileName = dataFileName;
        this.operator = operator;
        this.palaceName = palaceName;
        this.sysStartTime = sysStartTime;
        this.sysCounter = sysCounter;
        this.sysFrequency = sysFrequency;
        this.userIdName = userIdName;
        this.adFre = adFre;
        this.resolution = resolution;
        this.fileDuration = fileDuration;
        this.segmentNum = segmentNum;
        this.segmentHeadLength = segmentHeadLength;
        this.indexSegmentHeadLength = indexSegmentHeadLength;
        this.segmentRecNum = segmentRecNum;
        this.segmentDuration = segmentDuration;
        this.featureCode = featureCode;
        this.channelOnNum = channelOnNum;
        this.reserve = reserve;
    }

    public HFMEDHead() {

    }

    /*
    * getter and setter
    * */

    public short getFileHeadLength() {
        return fileHeadLength;
    }

    public void setFileHeadLength(short fileHeadLength) {
        this.fileHeadLength = fileHeadLength;
    }

    public String getFormatVer() {
        return formatVer;
    }

    public void setFormatVer(String formatVer) {
        this.formatVer = formatVer;
    }

    public String getDataFileName() {
        return dataFileName;
    }

    public void setDataFileName(String dataFileName) {
        this.dataFileName = dataFileName;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getPalaceName() {
        return palaceName;
    }

    public void setPalaceName(String palaceName) {
        this.palaceName = palaceName;
    }

    public byte[] getSysStartTime() {
        return sysStartTime;
    }

    public void setSysStartTime(byte[] sysStartTime) {
        this.sysStartTime = sysStartTime;
    }

    public long getSysCounter() {
        return sysCounter;
    }

    public void setSysCounter(long sysCounter) {
        this.sysCounter = sysCounter;
    }

    public long getSysFrequency() {
        return sysFrequency;
    }

    public void setSysFrequency(long sysFrequency) {
        this.sysFrequency = sysFrequency;
    }

    public String getUserIdName() {
        return userIdName;
    }

    public void setUserIdName(String userIdName) {
        this.userIdName = userIdName;
    }

    public Integer getAdFre() {
        return adFre;
    }

    public void setAdFre(Integer adFre) {
        this.adFre = adFre;
    }

    public short getResolution() {
        return resolution;
    }

    public void setResolution(short resolution) {
        this.resolution = resolution;
    }

    public Float getFileDuration() {
        return fileDuration;
    }

    public void setFileDuration(Float fileDuration) {
        this.fileDuration = fileDuration;
    }

    public Integer getSegmentNum() {
        return segmentNum;
    }

    public void setSegmentNum(Integer segmentNum) {
        this.segmentNum = segmentNum;
    }

    public short getSegmentHeadLength() {
        return segmentHeadLength;
    }

    public void setSegmentHeadLength(short segmentHeadLength) {
        this.segmentHeadLength = segmentHeadLength;
    }

    public short getIndexSegmentHeadLength() {
        return indexSegmentHeadLength;
    }

    public void setIndexSegmentHeadLength(short indexSegmentHeadLength) {
        this.indexSegmentHeadLength = indexSegmentHeadLength;
    }

    public int getSegmentRecNum() {
        return segmentRecNum;
    }

    public void setSegmentRecNum(int segmentRecNum) {
        this.segmentRecNum = segmentRecNum;
    }

    public float getSegmentDuration() {
        return segmentDuration;
    }

    public void setSegmentDuration(float segmentDuration) {
        this.segmentDuration = segmentDuration;
    }

    public String getFeatureCode() {
        return featureCode;
    }

    public void setFeatureCode(String featureCode) {
        this.featureCode = featureCode;
    }

    public short getChannelOnNum() {
        return channelOnNum;
    }

    public void setChannelOnNum(short channelOnNum) {
        this.channelOnNum = channelOnNum;
    }

    public int getReserve() {
        return reserve;
    }

    public void setReserve(int reserve) {
        this.reserve = reserve;
    }
}
