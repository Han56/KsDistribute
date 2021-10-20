package writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/10/20 下午8:10
 */
public class FlowMapper  extends Mapper<LongWritable, Text,Text,FlowBean> {

    private final Text outKey = new Text();
    private final FlowBean outValue = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取一行数据，转换为String
        String line = value.toString();

        //切割数据
        String[] split = line.split("\t");

        //抓取需要的数据【手机号、上行、下行】
        String numbers = split[1];
        String up = split[split.length-3];
        String down = split[split.length-2];

        //封装 OutK OutV
        outKey.set(numbers);
        outValue.setUpFlow(Long.parseLong(up));
        outValue.setDownFlow(Long.parseLong(down));
        outValue.setSumFlow();
        //写出
        context.write(outKey,outValue);
    }
}
