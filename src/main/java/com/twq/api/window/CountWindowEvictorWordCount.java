package com.twq.api.window;

import jdk.nashorn.internal.objects.Global;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*
    每个5s，统计前10s内单词出现的次数
 */
public class CountWindowEvictorWordCount {
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
        // 参数： wordGroup value类型，key类型，window的种类
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> keyedWindow =
                wordGroup
                // .countWindow(3, 2)
                .window(GlobalWindows.create())
                .trigger(new MyCountTrigger(2))
                .evictor(new MyCountEvictor(3L)); // 每两次触发计算，window是3个

        DataStream<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);

        // 4. Data Sink
        wordCounts.print().setParallelism(1);

        // 5. 启动并执行流程序
        env.execute("Window WordCount");
    }

    private static class MyCountEvictor
            implements Evictor<Tuple2<String, Integer>, GlobalWindow> {

        // window的大小
        private long windowCount;

        public MyCountEvictor(Long windowCount) {
            this.windowCount = windowCount;
        }

        /*
        在 window 计算之前 删除特定的数据
        elements: window中所有的元素
        size: window中所有元素的个数
        window:
        ctx: 上下文
         */
        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements,
                                int size,
                                GlobalWindow window,
                                EvictorContext ctx) {
            if (size <= windowCount) {
                return;
            }
            int evictorCount = 0;
            Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator();
            while (iterator.hasNext()) { // 边迭代，边删除
                iterator.next();
                evictorCount++;
                // 判断如果 删除的数量 小于 当前window大小 - 规定的window大小，就需要删除当前的元素
                if (evictorCount > size - windowCount) {
                    break;
                } else {
                    iterator.remove();
                }
            }
        }

        /*
        在 window 计算之后 删除特定的数据
        elements: window中所有的元素
        size: window中所有元素的个数
        window:
        ctx: 上下文
         */
        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {
            return;
        }
    }

    // 当接收到一定数量的时候，触发window计算
    private static class MyCountTrigger
            extends Trigger<Tuple2<String, Integer>, GlobalWindow> {

        // 表示指定元素的最大数量
        private long maxCount;

        // 用于存储每个 key 对应的count值
        private ReducingStateDescriptor<Long> stateDescriptor
                = new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long aLong, Long t1) throws Exception {
                return aLong + t1; // 每次来都要累加
            }
        }, Long.class);

        public MyCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        /*
        当一个元素进入到进入到一个window 中的时候，就会调用
        element: 进入的元素
        timestamp: 进来的时间
        window: 元素所属的窗口
        ctx: 上下文
        return: TriggerResult
            1. TriggerResult.CONTINUE: 表示对 window 不做任何处理
            2. TriggerResult.FIRE: 表示触发对 window 的计算
            3. TriggerResult.PURGE: 表示清除window中的所有数据
            4. TriggerResult.FIRE_AND_PURGE: 表示先触发window计算，然后删除window中的数据
         */
        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element,
                                       long timestamp,
                                       GlobalWindow window,
                                       TriggerContext ctx) throws Exception {
            // 拿到当前key对应的count状态值
            ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
            // count累加1
            count.add(1L);
            // 如果当前key的count值等于 maxCount 触发window计算
            if (count.get() == maxCount) {
                count.clear();
                //return TriggerResult.FIRE;
                return TriggerResult.FIRE;
            }
            // 否则对window不做任何处理
            return TriggerResult.CONTINUE;
        }

        /*
        onXXXTime: 注册定时器的时候用
            ctx.registerProcessingTimeTimer(n), 定时器触发的时候，会调用相应的
         */
        @Override
        public TriggerResult onProcessingTime(long timestamp,
                                              GlobalWindow window,
                                              TriggerContext ctx) throws Exception {
            // 写基于 Processing Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long timestamp,
                                         GlobalWindow window,
                                         TriggerContext ctx) throws Exception {
            // 写基于 Event Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

        // 清除状态值
        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDescriptor).clear();
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
