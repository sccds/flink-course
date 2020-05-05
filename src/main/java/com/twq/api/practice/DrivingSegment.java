package com.twq.api.practice;

import com.twq.api.DataFilePath;
import com.twq.api.datatypes.ConnectedCarEvent;
import com.twq.api.practice.function.CreateStopSegment;
import com.twq.api.practice.function.StopSegmentEvictor;
import com.twq.api.practice.function.StopSegmentOutOfOrderTrigger;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

/**
 * 实时计算每辆车的 StopSegment
 */
public class DrivingSegment {
    public static void  main(String[] args) throws Exception {
        // set up streaming env
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        // 需要使用 Event Time 做处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据源
        DataStream<String> carData = env
                .readTextFile(DataFilePath.CAR_EVENT_OUTOFORDER_PATH);

        // 字符串转成 ConnectedCarEvent
        DataStream<ConnectedCarEvent> events = carData
                .map((String line) -> ConnectedCarEvent.fromString(line))
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner());

        // 按照 carId 进行分组
        events.keyBy(event -> event.getCarId())
                .window(GlobalWindows.create())  // 先将分组之后的数据放在全局的窗口中
                .trigger(new StopSegmentOutOfOrderTrigger())  // 当 speed==0的事件来的时候，触发trigger
                .evictor(new StopSegmentEvictor()) // 计算完之后，要把stopsegment清除
                .process(new CreateStopSegment())
                .print();



        env.execute("DrivingSegment");
    }
}
