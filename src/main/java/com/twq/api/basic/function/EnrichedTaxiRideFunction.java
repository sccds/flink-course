package com.twq.api.basic.function;

import com.twq.api.datatypes.EnrichedTaxiRide;
import com.twq.api.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class EnrichedTaxiRideFunction implements FlatMapFunction<String, EnrichedTaxiRide> {
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