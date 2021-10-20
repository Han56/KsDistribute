package writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import wordCount.WordCountDriver;
import wordCount.WordCountMapper;
import wordCount.WordCountReducer;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/10/20 下午8:51
 */
public class FlowDrive {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置jar包路径
        job.setJarByClass(FlowDrive.class);

        //关联 Mapper/Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //设置map输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //设置最终输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path("/home/han56/hadoop_input/input_flow/"));
        FileOutputFormat.setOutputPath(job,new Path("/home/han56/hadoop_output/output_flow/"));
        //7.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }

}
