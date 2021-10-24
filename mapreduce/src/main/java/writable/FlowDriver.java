package writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/9/16 下午5:25
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2.设置jar包路径
        job.setJarByClass(FlowDriver.class);

        //3.关联Mapper/Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6.设置输入路径和输出路径(目录)
        FileInputFormat.setInputPaths(job,new Path("/home/han56/hadoop_input/input_flow"));
        FileOutputFormat.setOutputPath(job,new Path("/home/han56/hadoop_output/output3"));
        //7.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }

}
