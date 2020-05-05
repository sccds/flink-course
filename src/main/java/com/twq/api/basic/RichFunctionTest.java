package com.twq.api.basic;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RichFunctionTest {
    public static void main(String[] args) throws Exception {

        // 1. 初始化一个流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局参数 在客户端执行
        Configuration configuration = new Configuration();
        configuration.setString("testKey", "testValue");
        env.getConfig().setGlobalJobParameters(configuration);

        // 2. Data Source
        DataStreamSource<String> dataStreamSource = env.fromElements("this is an example", "this is my example");

        // 3. Data Process
        // 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数1
        DataStream<Tuple2<String, Integer>> wordOnes =
                dataStreamSource.flatMap(new WordOneFlatMapFunction()).name("flatMap operator");

        // 按照单词进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes.keyBy(0);

        // 聚合计算每个单词出现的次数
        DataStream<Tuple2<String, Integer>> wordCounts = wordGroup.sum(1);

        // 4. Data Sink
        wordCounts.print();

        // 5. 启动并执行流程序
        env.execute("Streaming WordCount");
    }

    private static class WordOneFlatMapFunction
            extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

        private String value = null;


        @Override
        public void flatMap(String line,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("flatMap :" + value);
            String[] words = line.toLowerCase().split(" ");
            for (String word : words) {
                Tuple2<String, Integer> wordOne = new Tuple2<>(word, 1);
                // 将单词计数1的二元组输出
                out.collect(wordOne);
            }
        }

        // 在 Operator SubTask 初始化时会调用一次
        // 仅仅就调用一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
            // 可以执行一次性的任务，比如
            // 1. 加载一次性的静态数据
            // 2. 建立和外部服务通讯的连接
            // 3. 访问配置参数 (Flink Batch API)

            // 还可以访问当前的 operator task 的运行时上下文
            RuntimeContext rc = getRuntimeContext();
            System.out.println(rc.getTaskName());

            // 获取全局参数
            ExecutionConfig.GlobalJobParameters globalJobParameters =
                    getRuntimeContext()
                            .getExecutionConfig()
                            .getGlobalJobParameters();
            Configuration configuration = (Configuration)globalJobParameters;
            value = configuration.getString("testKey", null);
        }

        // 当job结束的时候，每个 Operator SubTask 关闭时会调用一次
        // 仅仅就调用一次
        @Override
        public void close() throws Exception {
            System.out.println("close");
            // 用于关闭资源
        }
    }
}
