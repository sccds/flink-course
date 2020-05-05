package com.twq.api.practice.function;

import com.twq.api.datatypes.ConnectedCarEvent;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class StopSegmentOutOfOrderTrigger
        extends Trigger<ConnectedCarEvent, GlobalWindow> {
    @Override
    public TriggerResult onElement(ConnectedCarEvent element,
                                   long timestamp,
                                   GlobalWindow window,
                                   TriggerContext ctx) throws Exception {
        if (element.getSpeed() == 0.0) {
            // 事件是无序的，事件来的时候还不一定完全，所以不能发送trigger,可以注册定时器
            // 当 watermark 值达到 timestamp, 触发 OnEventTime方法，触发计算
            // 如果当 speed 等于0的话，还需要等待一下，等到 watermark 值等于当前元素的 eventtime就触发window计算
            ctx.registerEventTimeTimer(element.getTimestamp());
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time,
                                          GlobalWindow window,
                                          TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time,
                                     GlobalWindow window,
                                     TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

    }
}
