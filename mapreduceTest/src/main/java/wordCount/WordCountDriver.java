package wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述 写法固定
 * @create 2021/10/16 上午9:32
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        //关联 Mapper/Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置map输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最终输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置分区大小 【自定义
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/home/han56/hadoop_input/inputcombinetextinputformat/"));
        FileOutputFormat.setOutputPath(job,new Path("/home/han56/hadoop_output/combineText1"));
        //7.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }


}
