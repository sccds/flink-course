package com.twq;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class WordCountSource {
    public static void main(String[] args) throws Exception {
        // 1. 初始化一个流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Data Source
        //List<String> list = new ArrayList<>();
        //list.add("this is an example");
        //list.add("this is the first example");
        //DataStreamSource<String> dataStreamSource = env.readTextFile("data/text.txt");
        //DataStreamSource<String> dataStreamSource = env.fromCollection(list);
        DataStreamSource<String> dataStreamSource = env.fromElements("this is an example", "this is the first example");


        // 3. Data Process
        // 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数1
        DataStream<Tuple2<String, Integer>> wordOnes =
                dataStreamSource.flatMap(new WordOneFlatMapFunction());

        // 按照单词进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes.keyBy(0);

        // 聚合计算每个单词出现的次数
        DataStream<Tuple2<String, Integer>> wordCounts = wordGroup.sum(1);

        // 4. Data Sink
        wordCounts.print().setParallelism(1);

        // 5. 启动并执行流程序
        env.execute("Streaming WordCount Source");
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
