package com.twq.api.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
    每个5s，统计前10s内单词出现的次数
 */
public class WindowFunctionTest {
    public static void main(String[] args) throws Exception {
        // 1. 初始化一个流执行环境
        Configuration cfg = new Configuration();
        cfg.setInteger("rest.port", 50010);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 5001);

        DataStream<Tuple2<String, Integer>> users =
                dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] strings = s.split(",");
                        return Tuple2.of(strings[0], Integer.valueOf(strings[1]));
                    }
                });

        // keyed stream
        KeyedStream<Tuple2<String, Integer>, Tuple> userGroup = users
                .keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> keyedWindow =
                userGroup
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        DataStream<Tuple2<String, Integer>> result =
                keyedWindow.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> element1,
                                                      Tuple2<String, Integer> element2) throws Exception {
                    return Tuple2.of(element1.f0, element1.f1 + element2.f1);
                }
        });

        keyedWindow.aggregate(new AverageAggregate());

        keyedWindow.process(new MyProcessWindowFunction()).print();

        // 5. 启动并执行流程序
        env.execute("Window WordCount");
    }

    private static class MyProcessWindowFunction
        extends ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow> {

        /* 处理一个window中的所有元素，
         是按照单独的key来定义的计算逻辑，对于每个不同的Key都会调用process方法
         tuple: key
         ctx: 上下文
         elements: 当前key在当前window中的所有元素
         out: 用于输出
         */
        @Override
        public void process(Tuple tuple,
                            Context ctx,
                            Iterable<Tuple2<String, Integer>> elements,
                            Collector<String> out) throws Exception {
            // 统计相同的key出现的次数
            long count = 0;
            for (Tuple2<String, Integer> ele : elements) {
                count++;
            }
            // 输出
            out.collect("Window: " + ctx.window() + ", key: " + tuple + ", count: " + count);
        }
    }

    /*
    输入元素的类型
    中间的数据 ： 第一个元素表示user出现的个数，第二个元素表示value总值
    返回： double 平均值
     */
    private static class AverageAggregate
            implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Double> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        // 当key接收到元素之后，需要把element的值放到acc里面去
        @Override
        public Tuple2<Integer, Integer> add(Tuple2<String, Integer> element,
                                            Tuple2<Integer, Integer> accumulator) {
            return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + element.f1);
        }

        // 返回结果，平均值
        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return (double) accumulator.f1 / accumulator.f0;
        }

        // 合并两个相同的Key的不同 acc
        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a,
                                              Tuple2<Integer, Integer> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
