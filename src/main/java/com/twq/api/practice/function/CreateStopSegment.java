package com.twq.api.practice.function;

import com.twq.api.datatypes.ConnectedCarEvent;
import com.twq.api.datatypes.StoppedSegment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CreateStopSegment
        extends ProcessWindowFunction<ConnectedCarEvent, StoppedSegment, String, GlobalWindow> {
    @Override
    public void process(String carId,
                        Context context,
                        Iterable<ConnectedCarEvent> events,
                        Collector<StoppedSegment> out) throws Exception {
        StoppedSegment segment = new StoppedSegment(events);
        if (segment.getLength() > 0) {
            out.collect(segment);
        }
     }
}
