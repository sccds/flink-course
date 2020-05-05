package com.twq.api.basic;

import com.twq.api.DataFilePath;
import com.twq.api.basic.function.NYCFilter;
import com.twq.api.datatypes.EnrichedTaxiRide;
import com.twq.api.datatypes.TaxiRide;
import com.twq.api.source.GZIPFileSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;

public class CellDriverDuration implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // data source
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(TAXI_RIDE_PATH));

        // 2. 数据处理
        // 2.1 line -> EnrichedTaxiRide 计算每一个事件起始位置所有的网格id以及停止位置所属的网格id
        DataStream<EnrichedTaxiRide> enrichedTaxiRides =
                dataStreamSource.flatMap(new EnrichedTaxiRideFunction());

        // 2.2 计算出 END事件的起始位置所属网格 以及对应的 车开了多长时间
        // 即算出 <startCell, duration>: 从某一个单元格启动的车开了多长时间
        // 在每一个网格中启动的出租车 到停止 开车的时间
        DataStream<Tuple2<Integer, Integer>> minutesByStartCell =
                enrichedTaxiRides.flatMap(new FlatMapFunction<EnrichedTaxiRide, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(EnrichedTaxiRide ride, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                if (!ride.isStart()) {
                    // 车启动的经纬度所属的单元格
                    int startCell = ride.getStartCell();
                    Interval rideInterval = new Interval(ride.getStartTime(), ride.getEndTime());
                    // 车开了多长的时间
                    Integer duration = rideInterval.toDuration().toStandardMinutes().getMinutes();
                    out.collect(Tuple2.of(startCell, duration));
                }
            }
        });

        // 2.3 实时计算从每一个单元格启动的车开了最长的时间
        // 按照 startCell 分组，然后对duration聚合计算求最大值
        minutesByStartCell
                .keyBy(0) // startCell
                .maxBy(1)  // duration
                .print();

        env.execute("RideEnrichingWithFlatMap");
    }

    private static class EnrichedTaxiRideFunction
            implements FlatMapFunction<String, EnrichedTaxiRide> {
        @Override
        public void flatMap(String line,
                            Collector<EnrichedTaxiRide> out) throws Exception {
            // 将 line -> TaxiRide
            TaxiRide ride = TaxiRide.fromString(line);
            // 过滤
            NYCFilter filter = new NYCFilter();
            if (filter.filter(ride)) {
                // 直接输出 enriched 数据
                out.collect(new EnrichedTaxiRide(ride));
            }
        }
    }
}
