package wc;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author han56
 * @description 功能描述
 * flink流处理wordCount
 * @create 2021/11/8 上午10:06
 */
public class StreamingWordCount {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");

        int port=parameterTool.getInt("port");

        DataStream<String> inputStream = streamExecutionEnvironment.socketTextStream(host,port);

        DataStream<Tuple2<String,Integer>> wordCountDataStream = inputStream.flatMap(new WordCount.MyFlapMapper())
                .keyBy(0)
                .sum(1);

        wordCountDataStream.print().setParallelism(1);

        streamExecutionEnvironment.execute();

    }


}
