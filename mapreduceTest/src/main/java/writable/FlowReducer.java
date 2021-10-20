package writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/10/20 下午8:45
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean>{

    private final FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUp = 0;
        long totalDown = 0;

        //遍历 values ，将其中的上行流量，下行流量分开累加
        for (FlowBean flowBean:values){
            totalUp += flowBean.getUpFlow();
            totalDown += flowBean.getDownFlow();
        }

        //封装OutKV
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        //写出
        context.write(key,outV);
    }
}
