package com.twq.api.basic.function;

import com.twq.api.datatypes.TaxiRide;
import com.twq.api.util.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;

public class NYCFilter implements FilterFunction<TaxiRide>{
    @Override
    public boolean filter(TaxiRide ride) throws Exception {
        return GeoUtils.isInNYC(ride.getStartLon(), ride.getStartLat()) &&
                GeoUtils.isInNYC(ride.getEndLon(), ride.getEndLat());
    }
}
