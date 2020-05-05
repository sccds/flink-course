package com.twq.api.basic;

import com.twq.api.DataFilePath;
import com.twq.api.basic.function.NYCFilter;
import com.twq.api.datatypes.TaxiRide;
import com.twq.api.source.GZIPFileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCleaning implements DataFilePath {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // data source
        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource(TAXI_RIDE_PATH));

        // 数据处理

        // 将每一行字符串转成 TaxiRide 类型
        /*
        DataStream<TaxiRide> rides =
                dataStreamSource.map(new MapFunction<String, TaxiRide>() {
                    @Override
                    public TaxiRide map(String line) throws Exception {
                        return TaxiRide.fromString(line);
                    }
                });
         */
        // 用lambda表达式
        DataStream<TaxiRide> rides =
                dataStreamSource.map(line -> TaxiRide.fromString(line));

        // 过滤出起始位置和终点位置都在纽约的事件
        DataStream<TaxiRide> filteredRides = rides.filter(new NYCFilter());

        // 数据打印
        filteredRides.print();

        env.execute("RideCleaning");
    }

}
