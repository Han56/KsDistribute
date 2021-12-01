package vo;

import entity.*;

/**
 * @author han56
 * @description 功能描述
 * VO层：做存储之前的整合工作
 * @create 2021/11/29 下午2:38
 */
public class DataIntegration {

    public VOEntityClass intergration(DataElement dataElement,String winStartDate,String winEndDate,String fileName,int dataGruopNum){
        VOEntityClass voEntityClass = new VOEntityClass();

        voEntityClass.setDataElement(dataElement);
        //voEntityClass.setChannelInfo(channelInfo);
        voEntityClass.setDataGroupNum(dataGruopNum);
        voEntityClass.setFilePathName(fileName);
        //voEntityClass.setHfmedHead(hfmedHead);
        voEntityClass.setWinEndDate(winEndDate);
        //voEntityClass.setHfmedSegmentHead(hfmedSegmentHead);
        voEntityClass.setWinStartDate(winStartDate);

        return voEntityClass;

    }

}
