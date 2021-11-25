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
    public String formerDate() {
        return null;
    }

    @Override
    public void tailOfflineProcess() {

    }

    @Override
    public boolean voltProcessing(short voltValue, int loopCount) {
        return false;
    }
}
