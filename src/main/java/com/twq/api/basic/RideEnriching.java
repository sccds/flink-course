package com.twq.api.basic;

import com.twq.api.DataFilePath;
import com.twq.api.basic.function.NYCFilter;
import com.twq.api.datatypes.EnrichedTaxiRide;
import com.twq.api.datatypes.TaxiRide;
import com.twq.api.source.GZIPFileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideEnriching implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // data source
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(TAXI_RIDE_PATH));

        // 数据处理
        // line -> map + filter + map -> 可能是null 可能为1个 EnrichedTaxiRide
        // 可以用 flatMap实现

        // 将每一行字符串转成 TaxiRide 类型
        // 用lambda表达式
        DataStream<TaxiRide> rides =
                dataStreamSource.map(line -> TaxiRide.fromString(line));

        // 过滤出起始位置和终点位置都在纽约的事件
        DataStream<TaxiRide> filteredRides = rides.filter(new NYCFilter());

        // 计算每一个事件起始位置所有的网格id以及停止位置所属的网格id
        DataStream<EnrichedTaxiRide> enrichedTaxiRides = filteredRides.map(ride -> new EnrichedTaxiRide(ride));

        // 数据打印
        enrichedTaxiRides.print();

        env.execute("RideEnriching");
    }
}
