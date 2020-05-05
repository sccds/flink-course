package com.twq.api.practice;

import com.twq.api.DataFilePath;
import com.twq.api.datatypes.TaxiFare;
import com.twq.api.source.GZIPFileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


// 需求：实时计算每隔一个小时赚钱最多的司机
// 1. 计算出每个小时每个司机总共赚多少钱
// 2. 计算出赚钱最多的司机

public class HourlyTips implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 实时接收出租车收费事件
        DataStream<Tuple2<Long, Float>> driverIdWithFare =
                env.addSource(new GZIPFileSource(TAXI_FARE_PATH))
                .map(new MapFunction<String, Tuple2<Long, Float>>() {
                    @Override
                    public Tuple2<Long, Float> map(String s) throws Exception {
                        TaxiFare taxiFare = TaxiFare.fromString(s);
                        return Tuple2.of(taxiFare.getDriverId(), taxiFare.getTip());
                    }
                });
        
        // 1. 计算出每个小时每个司机总共赚了多少钱
        // keyed window, 可能会有若干个window, 多少个并行度就有多少个window
        DataStream<Tuple2<Long, Float>> hourlyTips = driverIdWithFare
                .keyBy(driverFare -> driverFare.f0)
                .timeWindow(Time.hours(1))
                .reduce(new ReduceFunction<Tuple2<Long, Float>>() {
                    // 相同 driverId 的结果加到临时结果中
                    @Override
                    public Tuple2<Long, Float> reduce(Tuple2<Long, Float> partialResult,
                                                      Tuple2<Long, Float> element) throws Exception {
                        return Tuple2.of(partialResult.f0, partialResult.f1 + element.f1);
                    }
                },
                // 当 window 触发时候的方法
                new ProcessWindowFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key,
                                        Context context,
                                        Iterable<Tuple2<Long, Float>> elements,
                                        Collector<Tuple2<Long, Float>> out) throws Exception {
                        // 拿到每个司机赚的钱。对于每个司机，这里只会有一条数据，因为前面聚合了
                        float sumOfTips = elements.iterator().next().f1;  // iterator里面的第一个值
                        out.collect(Tuple2.of(key, sumOfTips));
                    }
                });
                //.process(new AddTips());

        // 2. 计算赚钱最多的司机 这个时候需要将上面所有的window合并成一个window
        // non-keyed window
        DataStream<Tuple2<Long, Float>> houtlyMax = hourlyTips
                .timeWindowAll(Time.hours(1))
                .maxBy(1);

        houtlyMax.print();

        env.execute("Hourly Tips");
    }
}
