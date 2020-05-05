package com.twq.api.practice;

import com.twq.api.DataFilePath;
import com.twq.api.datatypes.ConnectedCarEvent;
import com.twq.api.practice.function.EventSortFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 实时对无序的Car Event 中的每一辆车所有的事件按照时间升序排列
// 1. 需要读取数据源，并将字符串转成 ConnectedCarEvent
// 2. 按照 carId 分组，然后对每个carId所有的事件按照时间 event time 升序排列
public class CarEventSort {
    public static void main(String[] args) throws Exception {
        // set up streaming env
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 需要使用 Event Time 做处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据源
        DataStream<String> carData = env
                .readTextFile(DataFilePath.CAR_EVENT_OUTOFORDER_PATH);

        // 字符串转成 ConnectedCarEvent
        DataStream<ConnectedCarEvent> events = carData
                .map((String line) -> ConnectedCarEvent.fromString(line))
                .assignTimestampsAndWatermarks(new ConnectedCarAssigner());

        // 对每一辆车事件按照 event time 排序
        SingleOutputStreamOperator<ConnectedCarEvent> sortedEvents =  events
                .keyBy(ConnectedCarEvent::getCarId)
                .process(new EventSortFunction());

        sortedEvents.getSideOutput(EventSortFunction.outputTag).print();
        //sortedEvents.print();

        env.execute("CarEventSort");
    }
}
