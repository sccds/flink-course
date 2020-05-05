package com.twq.api.practice.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AddTips
        extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple2<Long, Float>, Long, TimeWindow> {
    @Override
    public void process(Long key,
                        Context context,
                        Iterable<Tuple2<Long, Float>> elements,
                        Collector<Tuple2<Long, Float>> out) throws Exception {
        float sumOfTips = 0F;
        for (Tuple2<Long, Float> ele : elements) {
            sumOfTips += ele.f1;
        }
    }
}