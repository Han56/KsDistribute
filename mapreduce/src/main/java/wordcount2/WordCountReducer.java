package wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述：实现案例WordCount实例Reducer类
 * @create 2021/9/15 下午4:06
 */

/*
 * Reducer<>中的参数
 * KEYIN：reduce阶段输入的key的类型，LongWritable
 * VALUEIN：reduce阶段输入的value类型，Text
 * KEYOUT：reduce阶段输出的key类型，Text
 * VALUEOUT：reduce阶段输出的value类型，IntWritable
 * */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private final IntWritable outKey = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        //累加
        for (IntWritable value : values){
            sum += value.get();
        }

        //写出
        outKey.set(sum);
        context.write(key,outKey);

    }
}
