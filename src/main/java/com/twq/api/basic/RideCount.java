package com.twq.api.basic;

import com.twq.api.DataFilePath;
import com.twq.api.datatypes.EnrichedTaxiRide;
import com.twq.api.datatypes.TaxiRide;
import com.twq.api.source.GZIPFileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCount implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. data source
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(TAXI_RIDE_PATH));

        // 2. 数据处理

        // 2.1 计算每一行字符串转成 TaxiRide 类型
        // 2.2 过滤出是启动时间的 TaxiRide
        DataStream<TaxiRide> rides = dataStreamSource
                .map(line -> TaxiRide.fromString(line))
                .filter(ride -> ride.isStart());

        // 2.3 将每一个TaxiRide转换成<driverId, 1> 二元组类型
        DataStream<Tuple2<Long, Long>> driverOne =
                rides.map(ride -> Tuple2.of(ride.getDriverId(), 1L))
                .returns(Types.TUPLE(Types.LONG, Types.LONG)); // 类型有可能被擦除，用returns声明

        // 2.4 按照 driverId 进行分组
        // 2.5 聚合计算每一个司机开车的次数
        DataStream<Tuple2<Long, Long>> rideCounts = driverOne
                .keyBy(0)
                .sum(1);

        // 数据打印
        rideCounts.print();

        env.execute("RideCount");
    }
}
