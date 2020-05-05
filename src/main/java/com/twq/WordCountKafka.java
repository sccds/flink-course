package com.twq;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCountKafka {
    public static void main(String[] args) throws Exception {

        // 1. 初始化一个流执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 50010);
        // 本地环境 createLocal, 集群环境 getExecutionEnvironment
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration(conf));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Data Source
        // 从 kafka 中读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "flink-master:9092");
        properties.setProperty("group.id", "flink-test");
        FlinkKafkaConsumer<String> flinkKafkaConsumer =
                new FlinkKafkaConsumer<String>("flink-input",
                        new SimpleStringSchema(), properties);
        DataStreamSource<String> dataStreamSource = env.addSource(flinkKafkaConsumer, "kafkaSource");

        // 3. Data Process
        // 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数1
        DataStream<Tuple2<String, Integer>> wordOnes =
                dataStreamSource.flatMap(new WordOneFlatMapFunction());

        // 按照单词进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes.keyBy(0);

        // 聚合计算每个单词出现的次数
        DataStream<Tuple2<String, Integer>> wordCounts = wordGroup.sum(1);

        // 4. Data Sink
        // 将结果写到 kafka 中
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<String>("flink-output", new SimpleStringSchema(), properties);
        wordCounts.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.toString();
            }
        }).addSink(producer).name("kafkaSink").setParallelism(1);

        // 5. 启动并执行流程序
        env.execute("Streaming WordCount Kafka");
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
