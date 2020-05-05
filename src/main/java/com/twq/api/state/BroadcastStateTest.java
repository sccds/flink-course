package com.twq.api.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据流
        DataStreamSource<String> dataStreamSource =
                env.socketTextStream("localhost", 5001);

        // 控制流
        DataStreamSource<String> controlStreamSource =
                env.socketTextStream("localhost", 5002);

        // 解析控制流中的数据为二元组
        DataStream<Tuple2<String, Integer>> controlStream =
                controlStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] strings = s.split(" ");
                        return Tuple2.of(strings[0], Integer.valueOf(strings[1]));
                    }
                });

        // 将控制流中的数据 broadcast 到每一个task中
        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<String, Integer>(
                "lengthControl",
                String.class,
                Integer.class
        );
        BroadcastStream<Tuple2<String, Integer>> broadcastStream =
                controlStream.broadcast(descriptor);

        // 将数据流和控制流进行连接，利用控制流中的数据来控制字符串的长度
        dataStreamSource
                .connect(broadcastStream)
                .process(new LineLengthLimitProcessor())
                .print();

        env.execute("BroadcastTest");
    }

    private static class LineLengthLimitProcessor
            extends BroadcastProcessFunction<String, Tuple2<String, Integer>, String> {

        MapStateDescriptor<String, Integer> descriptor =
                new MapStateDescriptor<String, Integer>(
                    "lengthControl",
                    String.class,
                    Integer.class
        );

        @Override
        public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            // 从 broadcast state 中拿到控制信息
            Integer length = ctx.getBroadcastState(descriptor).get("length");
            if (length == null || value.length() <= length) {
                out.collect(value);
            }
        }

        @Override
        public void processBroadcastElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
            // 处理 broadcast 数据，也就是控制流
            // 将接收到的控制数据放到 broadcast state 中
            ctx.getBroadcastState(descriptor).put(value.f0, value.f1);

            // 每个 task 是否能收集到
            System.out.println(Thread.currentThread().getName() + " 接收到控制信息: " + value);
        }
    }
}
