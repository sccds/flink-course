package com.twq.api.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
    每个5s，统计前10s内单词出现的次数
 */
public class CountWindowWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 初始化一个流执行环境
        Configuration cfg = new Configuration();
        cfg.setInteger("rest.port", 50010);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg);

        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. Data Source
        // 从 socket 中读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 5001);

        // 3. Data Process
        // 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数1
        // non-keyed stream
        DataStream<Tuple2<String, Integer>> wordOnes =
                dataStreamSource.flatMap(new WordOneFlatMapFunction());
                        //.assignTimestampsAndWatermarks();

        // non-keyed window
        // 每隔 3s 计算所有单词的个数
        AllWindowedStream<Tuple2<String, Integer>, TimeWindow> nonKeyedWindow =
                wordOnes.timeWindowAll(Time.seconds(3));

        // 按照单词进行分组, 聚合计算每个单词出现的次数
        // keyed stream
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes
                .keyBy(0);

        // keyed window
        // 每隔 3s 计算每个单词出现的次数
        // 参数： wordGroup value类型，key类型，window的种类
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> keyedWindow =
                wordGroup
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(3)); // 每来3条数据，就会触发计算

        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);

        // 4. Data Sink
        wordCounts.print().setParallelism(1);

        // 5. 启动并执行流程序
        env.execute("Window WordCount");
    }

    private static class WordOneFlatMapFunction
            implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.toLowerCase().split(" ");
            for (String word : words) {
                Tuple2<String, Integer> wordOne = new Tuple2<>(word, 1);
                // 将单词计数1的二元组输出
                out.collect(wordOne);
            }
        }
    }
}
