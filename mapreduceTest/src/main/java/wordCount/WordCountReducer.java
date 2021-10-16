package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/10/16 上午9:24
 */

/*
 * Reducer<>中的参数
 * KEYIN：reduce阶段输入的key的类型，即Mapper阶段的 outKey 类型
 * VALUEIN：reduce阶段输入的value类型，即 Mapper 阶段的 outValue 类型
 * KEYOUT：reduce阶段输出的key类型，Text
 * VALUEOUT：reduce阶段输出的value类型，IntWritable
 * */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,Writable> {

    private final IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        //累加
        for (IntWritable value:values)
            sum+= value.get();

        //写出
        outValue.set(sum);
        //上下文写出
        context.write(key,outValue);
    }
}
