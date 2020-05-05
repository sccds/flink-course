package com.twq.api.basic;

import com.twq.api.DataFilePath;
import com.twq.api.basic.function.EnrichedTaxiRideFunction;
import com.twq.api.basic.function.NYCFilter;
import com.twq.api.datatypes.EnrichedTaxiRide;
import com.twq.api.datatypes.TaxiRide;
import com.twq.api.source.GZIPFileSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RideEnrichingWithFlatMap implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // data source
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(TAXI_RIDE_PATH));

        // 数据处理
        // line -> map + filter + map -> 可能是null 可能为1个 EnrichedTaxiRide
        // 可以用 flatMap实现

        // 计算每一个事件起始位置所有的网格id以及停止位置所属的网格id
        DataStream<EnrichedTaxiRide> enrichedTaxiRides = dataStreamSource.flatMap(new EnrichedTaxiRideFunction());

        // 数据打印
        enrichedTaxiRides.print();

        env.execute("RideEnrichingWithFlatMap");
    }

}
