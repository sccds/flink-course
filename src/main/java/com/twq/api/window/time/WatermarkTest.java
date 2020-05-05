package com.twq.api.window.time;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/*
    window 是什么时候执行的，window的执行条件

 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setAutoWatermarkInterval(1000);

        DataStreamSource<String> dataStreamSource =
                env.socketTextStream("localhost", 5001);

        // 保存迟到的，会被丢弃的数据
        OutputTag<Tuple2<String, Long>> outputTag =
                new OutputTag<Tuple2<String, Long>>("late-data"){};

        SingleOutputStreamOperator<String> window = dataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] strings = line.split(",");
                return Tuple2.of(strings[0], Long.valueOf(strings[1]));
            }
        })
                // 设置获取 EventTime 的逻辑
                .assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                //.allowedLateness(Time.seconds(2)) // 允许事件迟到2s
                .sideOutputLateData(outputTag) // 保存迟到太多的数据
                .process(new MyProcessWindowFunctrion());

        // 拿到迟到太多的数据
        DataStream<String> lateDataStream =
                window.getSideOutput(outputTag).map(new MapFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return "迟到的数据：" + stringLongTuple2.toString();
                    }
                });

        // 处理
        lateDataStream.print();

        window.print();


        env.execute("Watermark Test");
    }

    private static class EventTimeExtractor
            implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");


        private long currentMaxEventTime = 0L;  // 当前最大的eventtime
        private long maxOutOfOrderness = 10000; // 最大允许的乱序时间 10s

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            // 和事件关系不大
            // 1. watermark 值依赖处理时间的场景
            // 2. 当有一段时间没有接收到事件，但是仍然需要产生 watermark的场景
            return new Watermark(currentMaxEventTime - maxOutOfOrderness);
        }

        // 拿到每一个事件的EventTime
        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            long currentElementEventTime = element.f1;
            currentMaxEventTime = Math.max(currentElementEventTime, currentMaxEventTime);
            long currentThreadId = Thread.currentThread().getId();
            System.out.println("当前线程id: " + currentThreadId
                    + " event = " + element
                    + "|" + dateFormat.format(element.f1)  // event time
                    + "|" + dateFormat.format(currentMaxEventTime)  // max event time
                    + "|" + dateFormat.format(getCurrentWatermark().getTimestamp()) // current watermark
            );

            return currentElementEventTime;
        }
    }

    private static class EventTimeExtractor2
            implements AssignerWithPunctuatedWatermarks<Tuple2<String, Long>> {

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple2<String, Long> lastElement,
                                                  long extractedTimestamp) {
            // 每接收到一个事件，就会调用
            // 根据条件产生watermark, 并不是周期性的产生
            if (lastElement.f0 == "000002") {
                // 才发送 watermark
                return new Watermark(lastElement.f1 - 10000);
            }
            // 则表示不产生 watermark
            return null;
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element,
                                     long previousElementTimestamp) {
            return element.f1;
        }
    }

    private static class MyProcessWindowFunctrion
            extends ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(Tuple tuple, Context context,
                            Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) throws Exception {
            System.out.println("处理时间: " + dateFormat.format(context.currentProcessingTime()));
            System.out.println("window start time: " + dateFormat.format(context.window().getStart()));

            List<String> list = new ArrayList<>();
            for (Tuple2<String, Long> ele : elements) {
                list.add(ele.toString() + "|" + dateFormat.format(ele.f1));
            }
            out.collect(list.toString());

            System.out.println("window end time: " + dateFormat.format(context.window().getEnd()));
        }
    }
}
