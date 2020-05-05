package com.twq.api.practice.function;

import com.twq.api.datatypes.ConnectedCarEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.PriorityQueue;

/**
 * 思路：
 * 当每次接收到一个元素的时候，先将这个事件放到 PriorityQueue，按照 event time 进行升序排列
 * 当乱序的事件都到了，则触发定时器将排序好的事件输出
 */
public class EventSortFunction
        extends KeyedProcessFunction<String, ConnectedCarEvent, ConnectedCarEvent> {

    public static final OutputTag<String> outputTag = new OutputTag<String>("late_data"){};

    // 使用 PriorityQueue 对同一个car的所有事件进行排序
    private ValueState<PriorityQueue<ConnectedCarEvent>> queueValueState;

    // 注册状态
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>> descriptor =
                new ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>>(
                        "sorted-events",
                        TypeInformation.of(new TypeHint<PriorityQueue<ConnectedCarEvent>>(){})
                );
        queueValueState = getRuntimeContext().getState(descriptor);
    }

    // 每个事件来的时候，会调用
    @Override
    public void processElement(ConnectedCarEvent event,
                               Context ctx,
                               Collector<ConnectedCarEvent> out) throws Exception {
        // 拿到当前事件的 event time 和watermark
        long currentEventTime = ctx.timestamp();
        // 拿到当前的 watermark 值
        TimerService timerService = ctx.timerService();
        long currentWatermark = timerService.currentWatermark();

        if (currentEventTime > currentWatermark) {  // event 属于正常的数据
            PriorityQueue<ConnectedCarEvent> queue = queueValueState.value();
            if (queue == null) {
                queue = new PriorityQueue<>();
            }
            // 将事件放到优先级队列中
            queue.add(event);
            queueValueState.update(queue);

            // 当当前的watermark值到了当前的event的event time的时候，才触发定时器
            // 相当于每一个event等待了30s再输出
            timerService.registerProcessingTimeTimer(event.getTimestamp());
        } else { // 处理迟到太多的数据, 收集id
            ctx.output(outputTag, event.getId());
        }
    }

    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<ConnectedCarEvent> out) throws Exception {
        // 用于输出排好序的事件
        PriorityQueue<ConnectedCarEvent> queue = queueValueState.value();
        ConnectedCarEvent head = queue.peek();
        // 数据输出之前，先要比对一下当前的 watermark
        long currentWatermark = ctx.timerService().currentWatermark();
        // 输出数据，输出条件：
        // 1. 队列不能为空
        // 2. 拿出来的事件的 event time 需要小于当前的 watermark，防止把比watermark值大的数据输出
        while (head != null && head.getTimestamp() <= currentWatermark) {
            out.collect(head);
            queue.remove(head);
            head = queue.peek();
        }
    }
}
