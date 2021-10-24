package writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * @author han56
 * @description 功能描述：上下行流量Map类
 * @create 2021/9/16 下午4:55
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    private final Text outK = new Text();
    private final FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行信息
        String line = value.toString();

        //2.进行切割
        String[] words = line.split("\t");

        //3.抓取数据
        String phone_number = words[1];
        String upFlowStr = words[words.length-3];
        String downFlowStr = words[words.length-2];

        //4.封装数据
        outK.set(phone_number);
        outV.setUpFlow(Long.parseLong(upFlowStr));
        outV.setDownFlow(Long.parseLong(downFlowStr));
        outV.setSumFlow();

        //5.写出
        context.write(outK,outV);
    }
}
