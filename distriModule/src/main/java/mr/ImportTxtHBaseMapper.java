package mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/6 下午7:01
 */
public class ImportTxtHBaseMapper extends Mapper<LongWritable, Text,Text, Put> {

    @SuppressWarnings("deprecation")
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //从HDFS上读取数据
        String line = value.toString();

        //读取出来的每行数据用空格分割，并存储在String数组中
        String[] split = line.split(" ");

        //将组名作为rowKey
        String rowKey = split[0];

        String time = split[11]+split[12];

        String x1 = split[5];String x2 = split[6];String y1 = split[7];String y2 = split[8];
        String z1 = split[9];String z2 = split[10];

        String panfu = split[3].substring(48,49);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("time"),Bytes.toBytes("startTime"),Bytes.toBytes(time));
        put.addColumn(Bytes.toBytes(panfu),Bytes.toBytes("x1"),Bytes.toBytes(x1));
        put.addColumn(Bytes.toBytes(panfu),Bytes.toBytes("x2"),Bytes.toBytes(x2));
        put.addColumn(Bytes.toBytes(panfu),Bytes.toBytes("y1"),Bytes.toBytes(y1));
        put.addColumn(Bytes.toBytes(panfu),Bytes.toBytes("y2"),Bytes.toBytes(y2));
        put.addColumn(Bytes.toBytes(panfu),Bytes.toBytes("z1"),Bytes.toBytes(z1));
        put.addColumn(Bytes.toBytes(panfu),Bytes.toBytes("z2"),Bytes.toBytes(z2));

        context.write(new Text(rowKey),put);
    }



}
