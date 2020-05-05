package com.twq.api.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
    每个5s，统计前10s内单词出现的次数
 */
public class TimeWindowWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 初始化一个流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Data Source
        // 从 socket 中读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 5001);

        // 3. Data Process
        // 对每一行按照空格切割，得到所有单词，并且可以对每个单词先计数1
        DataStream<Tuple2<String, Integer>> wordOnes =
                dataStreamSource.flatMap(new WordOneFlatMapFunction());

        // 按照单词进行分组, 聚合计算每个单词出现的次数
        DataStream<Tuple2<String, Integer>> wordCounts = wordOnes
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new SumProcessWindowFunction()); // 和 sum 效果一样

        // 4. Data Sink
        wordCounts.print().setParallelism(1);

        // 5. 启动并执行流程序
        env.execute("Streaming Time Window WordCount");
    }

    private static class SumProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow> {

        private FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        // 当 window 触发计算的时候会调用这个方法，处理一个window中单个key对应的所有元素，window中的每个key会调用一次
        // tuple: key
        // context: operator 上下文
        // elements: window中指定的key对应的所有元素
        // out: 用于输出
        @Override
        public void process(Tuple tuple, Context context,
                            Iterable<Tuple2<String, Integer>> elements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("--------------");
            System.out.println("当前系统时间: " + dateFormat.format(System.currentTimeMillis()));
            System.out.println("window 处理时间: " + dateFormat.format(context.currentProcessingTime()));
            System.out.println("window的开始时间: " + dateFormat.format(context.window().getStart()));
            System.out.println("window的结束时间: " + dateFormat.format(context.window().getEnd()));

            int sum = 0;
            for (Tuple2<String, Integer> ignored : elements) {
                sum += 1;
            }
            out.collect(Tuple2.of(tuple.getField(0), sum));
        }
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
