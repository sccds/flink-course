package com.twq.api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GZIPFileSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.addSource(new GZIPFileSource("data/taxi/nycTaxiRides.gz"));
        dataStreamSource.print().setParallelism(1);

        env.execute("GZIPFileSourceTest");
    }
}
