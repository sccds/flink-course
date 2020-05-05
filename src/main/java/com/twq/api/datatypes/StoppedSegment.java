package com.twq.api.datatypes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 从车的起始开始到speed=0，发出的所有的事件
 */
public class StoppedSegment extends Segment {
    public StoppedSegment(Iterable<ConnectedCarEvent> events) {
        List<ConnectedCarEvent> list = new ArrayList<>();
        for (Iterator<ConnectedCarEvent> iterator = events.iterator(); iterator.hasNext();) {
            ConnectedCarEvent event = iterator.next();
            if (event.getSpeed() != 0.0) {  // speed=0的事件不算
                list.add(event);
            }
        }
        this.setLength(list.size());
        if (this.getLength() > 0) {
            this.setCarId(list.get(0).getCarId());
            this.setStartTime(Segment.minTimestamp(list));
            this.setMaxSpeed(Segment.maxSpeed(list));
            this.setErraticness(Segment.stddevThrottle(list));
        }
    }
}
