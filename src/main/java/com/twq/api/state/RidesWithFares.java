package com.twq.api.state;

import com.twq.api.datatypes.TaxiFare;
import com.twq.api.datatypes.TaxiRide;
import com.twq.api.source.GZIPFileSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import static com.twq.api.DataFilePath.TAXI_FARE_PATH;
import static com.twq.api.DataFilePath.TAXI_RIDE_PATH;

// 根据 RideId 实时关联  TaxiRide 和 TaxiFare 事件
public class RidesWithFares {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 默认 5M
        MemoryStateBackend memoryStateBackend = new MemoryStateBackend(100 * 1024 * 1024);

        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://master:9001/checkpoint_path/");

        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://master:9001/checkpoint_path/");

        env.setStateBackend(memoryStateBackend);

        // 设置 checkpoint
        // 开启 checkpoint , checkpoint 周期 x ms
        env.enableCheckpointing(10000);
        // 配置 checkpoint 行为特性
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 设置两个 checkpoint 之间必须间隔一段时间, 最小间隔时间30s
        checkpointConfig.setMinPauseBetweenCheckpoints(30000);
        // 设置可以允许多个checkpoint一起运行，前提是checkpoint不占资源
        checkpointConfig.setMaxConcurrentCheckpoints(3);
        // 设置可以给 checkpoint 设置超时时间，如果达到了超时时间，Flink会强制丢弃这一次checkpoint
        // 默认值是 10 min
        checkpointConfig.setCheckpointTimeout(30000);
        // 设置 即使checkpoint出错，继续让程序正常运行， 1.9.0不建议使用
        // checkpointConfig.setFailOnCheckpointingErrors(false);
        checkpointConfig.setTolerableCheckpointFailureNumber(Integer.MAX_VALUE); // 默认是 0
        // 设置 当flink程序取消的时候，保留checkpoint数据
        // CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        // CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置 Flink 程序的自动重启策略
        // 默认是 fixed-delay, 重启的次数是 Integer.MAX_VALUE, 时间间隔是 10s
        // 1. fixed-delay restart strategy: 尝试重启多次，每次重启之间有一个时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                5, // 重新启动总次数
                Time.seconds(30) // 每次重新启动的时间间隔
        ));
        // 2. failure-rate restart strategy: 尝试在一个时间段内，重启执行次数，每次重启之间也需要一个时间间隔
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                5,        // 在指定时间段的重启次数
                Time.seconds(30),   // 指定的时间段
                Time.seconds(5)     // 两次重启之间的时间间隔

        ));
        // 3. no-restart strategy: 不进行重启
        env.setRestartStrategy(RestartStrategies.noRestart());


        // 读取 TaxiRide 数据
        KeyedStream<TaxiRide, Long> rides = env.addSource(new GZIPFileSource(TAXI_RIDE_PATH))
                .map(line -> TaxiRide.fromString(line))
                .filter(ride -> ride.isStart())  // 只使用启动事件
                .keyBy(ride -> ride.getRideId());

        // 读取 TaxiFare 数据
        KeyedStream<TaxiFare, Long> fares = env.addSource(new GZIPFileSource(TAXI_FARE_PATH))
                .map(TaxiFare::fromString)
                .keyBy(fare -> fare.getRideId());

        // 根据相同 rideId 进行关联
        rides
            .connect(fares)
            .flatMap(new EnrichmentFunction())
            //.print();
            .addSink(new CustomSink(20));

        env.execute("RidesWithFares");
    }

    private static class EnrichmentFunction
            extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

        // 记住相同的 rideId 对应的 TaxiRide 事件
        private ValueState<TaxiRide> rideValueState;

        // 记住相同的 rideId 对应的 TaxiFare 事件
        private ValueState<TaxiFare> fareValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            rideValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<TaxiRide>("saved ride", TaxiRide.class));

            fareValueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<TaxiFare>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // 处理相同 rideId 对应的 TaxiRide 事件
            // 先要看一下 rideId 对应的 TaxiFare 是否已经存在状态中
            TaxiFare fare = fareValueState.value();
            if (fare != null) { // 说明对应的 rideId 的 TaxiFare 事件已经到达
                fareValueState.clear();
                // 组合起来发送
                out.collect(Tuple2.of(ride, fare));
            } else {
                // 把 ride 先保存到 rideValueState 中
                rideValueState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // 处理相同 rideId 对应的 TaxiFare 事件
            // 先要看一下 rideId 对应的 TaxiRide 是否已经存在状态中
            TaxiRide ride = rideValueState.value();
            if (ride != null) { // 说明对应的 rideId 的 TaxiRide 事件已经到达
                rideValueState.clear();
                // 组合起来发送
                out.collect(Tuple2.of(ride, fare));
            } else {
                // 把 fare 先保存到 fareValueState 中
                fareValueState.update(fare);
            }
        }
    }
}
