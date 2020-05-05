package com.twq.api.practice.function;

import com.twq.api.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

public class StopSegmentEvictor
        implements Evictor<ConnectedCarEvent, GlobalWindow> {
    @Override
    public void evictBefore(Iterable<TimestampedValue<ConnectedCarEvent>> elements,
                            int size,
                            GlobalWindow window,
                            EvictorContext ctx) {

    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<ConnectedCarEvent>> elements,
                           int size,
                           GlobalWindow window,
                           EvictorContext ctx) {
        // 找到最近 speed=0 的时间，把前面的数据都清掉
        long earliestTime = Long.MAX_VALUE;
        for (Iterator<TimestampedValue<ConnectedCarEvent>> iterator = elements.iterator(); iterator.hasNext();) {
            TimestampedValue<ConnectedCarEvent> element = iterator.next();
            if (element.getTimestamp() < earliestTime
                    && element.getValue().getSpeed() == 0.0) {
                earliestTime = element.getTimestamp();
            }
        }
        // 删除小于等于最近停止事件的时间的事件
        for (Iterator<TimestampedValue<ConnectedCarEvent>> iterator = elements.iterator(); iterator.hasNext();) {
            TimestampedValue<ConnectedCarEvent> element = iterator.next();
            if (element.getTimestamp() <= earliestTime) {
                iterator.remove();
            }
        }
    }
}
