package com.twq.api.practice;

import com.twq.api.DataFilePath;
import com.twq.api.datatypes.ConnectedCarEvent;
import com.twq.api.datatypes.GapSegment;
import com.twq.api.practice.function.CreateStopSegment;
import com.twq.api.practice.function.StopSegmentEvictor;
import com.twq.api.practice.function.StopSegmentOutOfOrderTrigger;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 实时计算每辆车的 StopSegment
 */
public class DrivingSessions {
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
                .window(EventTimeSessionWindows.withGap(Time.seconds(15)))  // 先将分组之后的数据放在全局的窗口中
                .process(new ProcessWindowFunction<ConnectedCarEvent, GapSegment, String, TimeWindow>() {
                    @Override
                    public void process(String carId,
                                        Context context,
                                        Iterable<ConnectedCarEvent> elements,
                                        Collector<GapSegment> out) throws Exception {
                        GapSegment segment = new GapSegment(elements);
                        out.collect(segment);
                    }
                })
                .print();



        env.execute("DrivingSegment");
    }
}
