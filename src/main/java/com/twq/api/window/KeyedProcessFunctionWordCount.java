package com.twq.api.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
    每个5s，统计前10s内单词出现的次数
 */
public class KeyedProcessFunctionWordCount {
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


        // 按照单词进行分组, 聚合计算每个单词出现的次数
        // keyed stream
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes
                .keyBy(0);

        // keyed window


        DataStream<Tuple2<String, Integer>> wordCounts = wordGroup
                //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple2<String, Integer>>() {
                    @Override
                    public long extract(Tuple2<String, Integer> element) {
                        if (element.f0.equals("this")) {
                            return 10000;
                        } else if (element.f0.equals("is")) {
                            return 20000;
                        }
                        return 5000;
                    }
                }))
                .sum((1));
                //.process(new CountWithTimeoutFunction());

        // 4. Data Sink
        wordCounts.print().setParallelism(1);

        // 5. 启动并执行流程序
        env.execute("KeyedProcessFUnction WordCount");
    }

    // KeyedProcessFunction: key的类型，输入元素类型，输出元素类型
    private static class CountWithTimeoutFunction
            extends KeyedProcessFunction<Tuple, Tuple2<String,Integer>, Tuple2<String,Integer>> {

        private ValueState<CountWithTimestamp> state;

        // 注册状态
        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext()
                    .getState(new ValueStateDescriptor<CountWithTimestamp>(
                            "myState",
                            CountWithTimestamp.class));

        }

        // 用于处理每一个接收到的单词（元素）
        @Override
        public void processElement(Tuple2<String, Integer> element,
                                   Context ctx,
                                   Collector<Tuple2<String, Integer>> out) throws Exception {
            // 先拿到当前 key 的 对应的状态
            CountWithTimestamp currentState = state.value();
            if (currentState == null) {
                // 第一次来
                currentState = new CountWithTimestamp();
                currentState.key = element.f0;
            }
            // 更新这个 key 出现的次数
            currentState.count++;

            // 更新这个 key 到达的时间，最后修改这个状态时间为当前的 Processing Time
            currentState.lastModified = ctx.timerService().currentProcessingTime();

            // 更新状态
            state.update(currentState);

            // 注册一个定时器
            // 注册一个以processing time 为准的定时器
            // 定时器触发的时间是当前 key 的最后修改时间 + 5s
            ctx.timerService()
                    .registerProcessingTimeTimer(currentState.lastModified + 5000);
        }

        // 定时器需要运行的逻辑。
        // 前面注册了定时器，当定时器时间到达的时候，定时器里面需要执行的东西
        // input: 定时器触发的时间戳，上下文，用于输出
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            // 先拿到当前key的状态
            CountWithTimestamp curr = state.value();
            // 检查这个Key是不是5秒钟没有接收到数据，触发时间是否等于 lastmodifiedtime + 5000
            if (timestamp == curr.lastModified + 5000) {
                out.collect(Tuple2.of(curr.key, curr.count));
                state.clear();
            }
        }
    }

    // 写一个类，把状态封装到类中
    private static class CountWithTimestamp {
        String key;
        int count;
        long lastModified;
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
