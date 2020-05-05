package com.twq.api.state;

import com.twq.api.datatypes.TaxiFare;
import com.twq.api.datatypes.TaxiRide;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class CustomSink implements SinkFunction<Tuple2<TaxiRide, TaxiFare>>, CheckpointedFunction {

    // 用于缓存结果数据的
    private List<Tuple2<TaxiRide, TaxiFare>> bufferElements;
    // 表示内存中数据的大小阈值
    private int threshold;

    // 用于保存内存中的状态信息
    private ListState<Tuple2<TaxiRide, TaxiFare>> checkpointState;
    // StateBackend, checkpoint

    public CustomSink(int threshold) {
        this.threshold = threshold;
        this.bufferElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<TaxiRide, TaxiFare> value, Context context) throws Exception {
        // 可以将接收到的每一条数据保存到任何存储系统中
        bufferElements.add(value);
        if (bufferElements.size() == threshold) {
            System.out.println("自定义格式: " + bufferElements);
            bufferElements.clear();
        }
    }

    // 用于将内存中数据保存到状态中
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointState.clear();
        for (Tuple2<TaxiRide, TaxiFare> ele : bufferElements) {
            checkpointState.add(ele);
        }
    }

    // 用于在程序恢复的时候从状态中恢复数据到内存
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 注册
        ListStateDescriptor<Tuple2<TaxiRide, TaxiFare>> descriptor =
                new ListStateDescriptor<Tuple2<TaxiRide, TaxiFare>>(
                        "buffered elements",
                        TypeInformation.of(new TypeHint<Tuple2<TaxiRide, TaxiFare>>() {}));
        // 注册一个 operator state
        checkpointState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            // 程序出错时从状态中恢复到内存
            for (Tuple2<TaxiRide, TaxiFare> ele : checkpointState.get()) {
                bufferElements.add(ele);
            }
        }
    }
}
