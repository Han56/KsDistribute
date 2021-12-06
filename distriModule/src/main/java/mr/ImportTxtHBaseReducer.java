package mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author han56
 * @description 功能描述
 * @create 2021/12/6 下午8:32
 */
public class ImportTxtHBaseReducer extends TableReducer<Text, Put, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //读出来一行 数据写入到 1909res table
        for(Put put:values)
            context.write(NullWritable.get(),put);
    }
}
