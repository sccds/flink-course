package com.twq.api.state;

import com.twq.api.state.function.CountWindowAverageWithListState;
import com.twq.api.state.function.CountWindowAverageWithMapState;
import com.twq.api.state.function.CountWindowAverageWithValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


// 需求：当接收到的相同key的元素个数等于3个或者超过3个的时候，就计算这些元素的value的平均值
// 计算 keyed stream 中每3个元素的value的平均值
// 1. ValueState 保存的是对应的一个Key的状态值

public class TestStatefulApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(
                        Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                        Tuple2.of(1L, 7L), Tuple2.of(2L, 4L),
                        Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));

        // 输出：
        // (1, 5.0)
        // (2, 3.66666)

        dataStreamSource
                .keyBy(0)
                //.flatMap(new CountWindowAverageWithValueState())
                //.flatMap(new CountWindowAverageWithListState())
                .flatMap(new CountWindowAverageWithMapState())
                .print();

        env.execute("TestStatefulApi");
    }


}
