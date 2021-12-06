package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/6 下午7:35
 */
public class ImportTxtHBaseDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
        configuration.set("hbase.zookeeper.property.clientPort","2181");

        Job job = Job.getInstance(configuration,ImportTxtHBaseDriver.class.getSimpleName());

        //已经在HBase中建好的表
        TableMapReduceUtil.initTableReducerJob("1909res",ImportTxtHBaseReducer.class,job);

        job.setJarByClass(ImportTxtHBaseDriver.class);
        job.setMapperClass(ImportTxtHBaseMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Put.class);
        //hdfs://hadoop101:8020//hy_history_data/readedRes/group1/190925110520res.txt
        FileInputFormat.addInputPath(job,new Path("/data/files/DownLoads/KsDisOut/group1/190925110520res.txt"));
        job.setOutputFormatClass(TableOutputFormat.class);
        job.waitForCompletion(true);
    }

}
