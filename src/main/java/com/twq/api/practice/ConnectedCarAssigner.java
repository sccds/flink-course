package com.twq.api.practice;

import com.twq.api.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * watermark 产生机制：
 * 1. 根据一些条件产生，每次接收到一个事件的话，就会产生一个 watermark
 */
public class ConnectedCarAssigner
        implements AssignerWithPunctuatedWatermarks<ConnectedCarEvent> {

    // 调用这个方法，把event time 做延时逻辑，生成 watermark
    // 需要注意，watermark 在以下两种情况下是不会变化的：
    // 1. 返回 null
    // 2. 当前返回的 watermark值 比上一次返回的 watermark值还要小
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(ConnectedCarEvent lastElement,
                                              long extractedTimestamp) {
        // 设置 watermark 值为当前事件的 event time 减去 30s, 允许迟到30s
        return new Watermark(extractedTimestamp - 30000);
    }

    // 每个事件来的时候，调用这个方法，拿到 event time
    @Override
    public long extractTimestamp(ConnectedCarEvent element, long previousElementTimestamp) {
        return element.getTimestamp();
    }
}
