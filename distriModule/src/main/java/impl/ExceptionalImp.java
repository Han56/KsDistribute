package impl;

/**
 * @author han56
 * @description 功能描述
 * 处理读取文件过程中异常情况的操作方法集合
 * @create 2021/11/17 下午8:56
 */
public interface ExceptionalImp {

    //处理GPS压力跳秒加一问题
    String formerDate(String segDate,int timeCount);

    //读取文件到末尾的操作
    void tailOfflineProcess();

    //压力值处理，当出现高电平向低电平过度时，认为是一秒的结束
    boolean voltProcessing(short voltValue,int loopCount);


}
