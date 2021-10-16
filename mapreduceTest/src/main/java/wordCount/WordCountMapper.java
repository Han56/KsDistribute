package wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/10/16 上午9:17
 */

/*
 * Mapper<>中的参数
 * KEYIN：map阶段输入的key的类型，LongWritable
 * VALUEIN：map阶段输入的value类型，Text
 * KEYOUT：map阶段输出的key类型，Text
 * VALUEOUT：map阶段输出的value类型，IntWritable
 * */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();

        //对这一行数据按照空格切分，具体情况具体分析
        String[] words = line.split(" ");

        //循环写出
        for (String word:words){
            //封装outKey
            outKey.set(word);
            //上下文写出
            context.write(outKey,outValue);
        }
    }
}
