package com.tv.streamwithflink.flinkstate;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Description 如果你的应用需要用到键值域不断变化的键值分区状态，那么必须确保那些无用的状态能够被删除。
 * 该工作可以通过注册一个针对未来某个时间点的timer来完成。
 * @Author Allen
 * @Date 2020-12-24 17:28
 **/
public class StateProcessFunction {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 配置重启策略
        env.setRestartStrategy(
                RestartStrategies
                        .fixedDelayRestart(3, Time.of(20, TimeUnit.SECONDS)));

        env.enableCheckpointing(10_000L);

     /*   try {
            // 设置状态后端
            String checkpointPath = "file:///Users/allen/bigdataapp/flinktutorial/temp";
            StateBackend backend = new RocksDBStateBackend(checkpointPath);
            env.setStateBackend(backend);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }*/

        env.getConfig().setAutoWatermarkInterval(1000L);
        DataStream<SensorReading> originSource = env.addSource(new SensorSource());

        DataStream<SensorReading> sensorData = originSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((sensorReading, timestamp) -> {
                    return sensorReading.getTimestamp();
                }));

        /**
         * org.apache.flink.streaming.api.datastream.KeyedStream
         *
         * A {@link KeyedStream} represents a {@link DataStream} on which operator state is
         * partitioned by key using a provided {@link KeySelector}.
         *
         * Typical operations supported by a {@code DataStream} are also possible on a {@code KeyedStream}, with the exception of
         * partitioning methods such as shuffle, forward and keyBy.
         *
         * <p>Reduce-style operations, such as {@link #reduce}, {@link #sum} and {@link #fold} work on
         * elements that have the same key.
         *
         * @param <T> The type of the elements in the Keyed Stream.
         * @param <KEY> The type of the key in the Keyed Stream.
         */
        KeyedStream<SensorReading, String> keyedSensorData = sensorData.keyBy(value -> value.getId());

        DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData.process(new TempProccessWithSelfCleaningFunction(1.5d));

        alerts.print();

        try {
            env.execute("Generate Temperature Alerts With State Clearing");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

/**
 * 在某一键值超过1小时没有新数据到来时，将该键值对应的状态清除
 */
class TempProccessWithSelfCleaningFunction extends KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>>{

    private Double threshold;

    public TempProccessWithSelfCleaningFunction(Double threshold) {
        this.threshold = threshold;
    }

    // 当前键分区流的最后一个元素的温度值
    private ValueState<Double> lastTemp;
    // 注册 Timer 的时间戳
    private ValueState<Long> timerTimestamp;
    // lastTemp state descriptor
    //private ValueStateDescriptor<Double> lastTempDescriptor = new ValueStateDescriptor<Double>("lastTemp", TypeInformation.of(Double.class));
    // timerTimestamp state descriptor
    //private ValueStateDescriptor<Long> timerTimestampDescriptor = new ValueStateDescriptor<Long>("timerTimestamp", TypeInformation.of(Long.TYPE));

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", TypeInformation.of(Double.class)));
        timerTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTimestamp", TypeInformation.of(Long.TYPE)));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {

        System.out.println("sensor id===== "+value.getId()+"， timestamp===== "+value.getTimestamp());
        /**
         *  新的计时器的触发时间设为： 当前记录的时间戳晚  20s
         */
        // Long newTimerTimestamp = value.getTimestamp() + 60_000;
        Long newTimerTimestamp = ctx.timestamp() + 1000L;
        System.out.println("newTimerTimestamp===== "+newTimerTimestamp);
        // 删除上一个时间戳位置的 Timer
        if(timerTimestamp.value() != null){
            /**
             * Registers a timer to be fired when the event time watermark passes the given time.
             *
             * <p>Timers can internally be scoped to keys and/or windows. When you set a timer
             * in a keyed context, such as in an operation on
             * {@link org.apache.flink.streaming.api.datastream.KeyedStream} then that context
             * will also be active when you receive the timer notification.
             */
            ctx.timerService().deleteEventTimeTimer(timerTimestamp.value());
        }
        // 注册新的 Timer
        ctx.timerService().registerEventTimeTimer(newTimerTimestamp);

        // 更新新的 Timer的触发时间到状态中
        timerTimestamp.update(newTimerTimestamp);

        if(lastTemp.value() != null){
            Double diff = Math.abs(value.getTemperature() - lastTemp.value());
            if(diff > threshold){
                out.collect(new Tuple3<String, Double, Double>(value.getId(), value.getTemperature(), diff));
            }
        }
        // 更新键值流中最后一个记录的 temperature
        lastTemp.update(value.getTemperature());
    }

    /**
     * 设置计时器回调函数，  System.out.println("clear KeyedPartition state success, key: "+ctx.getCurrentKey()); 打印不了的原因？
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        /**
         * Removes the value mapped under the current key.
         */
        lastTemp.clear();
        timerTimestamp.clear();
        System.out.println("clear KeyedPartition state success, key: "+ctx.getCurrentKey()+"， timestamp: "+timestamp);
    }

}
