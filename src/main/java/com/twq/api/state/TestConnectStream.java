package com.twq.api.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class TestConnectStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 控制信号流
        DataStream<String> control =
                env.fromElements("DROP", "IGNORE").keyBy(x -> x);

        // 单词流，包含控制信号
        DataStream<String> streamOfWords =
                env.fromElements("data", "DROP", "artisans", "IGNORE").keyBy(x -> x);

        // 单词流中过滤掉控制信号
        // 将控制流中的数据放到某个状态中，数据输入流从状态中查询单词是否存在，存在的话则不输出
        // 输出： artisans, data

        control
            .connect(streamOfWords)
            .flatMap(new ControlFunction())
            .print();

        env.execute("TestConnectStream");
    }

    private static class ControlFunction
        extends RichCoFlatMapFunction<String, String, String> {

        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration parameters) {
            // 进行注册
            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor("blocked", Boolean.class);
            blocked = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap1(String controlElement, Collector<String> out) throws Exception {
            // 接收到控制流数据的时候会触发的计算
            // 如果接收到控制流数据，直接设置状态为true
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String dataValue, Collector<String> out) throws Exception {
            // 接收到数据输入流的时候会触发的计算
            // 如果当前key(单词)不在状态的话，也就是控制流中不存在，输出数据

            if (blocked.value() == null || !blocked.value()) { // 控制流中不存在
                out.collect(dataValue);
            }
        }
    }
}
