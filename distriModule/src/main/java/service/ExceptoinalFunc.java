package service;

import impl.ExceptionalImp;

/**
 * @author han56
 * @description 功能描述
 * 实现异常处理的具体方法
 * @create 2021/11/18 下午4:35
 */
public class ExceptoinalFunc implements ExceptionalImp {

    @Override
    public String formerDate(String segDate, int timeCount) {
        return null;
    }

    @Override
    public boolean tailOfflineProcess(int by,int channelNums) {
        return false;
    }

    @Override
    public boolean voltProcessing(short voltValue, int loopCount) {
        return false;
    }
}
