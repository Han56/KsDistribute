package wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author han56
 * @description 功能描述
 * flink批处理WordCount
 * @create 2021/11/8 上午9:06
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取数据
        String inputPath = "/data/workarea/idea/KsDistribute/studyFlink/src/main/java/testFiles/hello.txt";

        DataSet<String> inputDataSet = environment.readTextFile(inputPath);

        //空格分词打开后，对单词进行 groupby 分组，然后用 sum 进行分组
        DataSet<Tuple2<String,Integer>> wordCountDataSet = inputDataSet.flatMap(new MyFlapMapper())
                .groupBy(0)
                .sum(1);
        //打印输出
        wordCountDataSet.print();
    }

    public static class MyFlapMapper implements FlatMapFunction<String,Tuple2<String,Integer>>{

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word:words)
                out.collect(new Tuple2<String, Integer>(word,1));
        }
    }

}
