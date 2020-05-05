package com.twq.api.state;

import com.twq.api.state.function.ContainsValueFunction;
import com.twq.api.state.function.CountWindowAverageWithMapState;
import com.twq.api.state.function.SumFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


// 需求：key 求总值, 用reduce实现sum

public class TestStatefulApi2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(
                        Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L), Tuple2.of(2L, 4L),
                        Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));

        dataStreamSource
                .keyBy(0)
                //.flatMap(new SumFunction())
                .flatMap(new ContainsValueFunction())
                .print();

        env.execute("TestStatefulApi2");
    }
}
