package com.tv.streamwithflink.flinkstate;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author Allen
 * @Date 2020-12-23 14:31
 **/
public class CheckpointedFunction007 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        // 配置重启策略
        env.setRestartStrategy(
                RestartStrategies
                        .fixedDelayRestart(3, Time.of(20, TimeUnit.SECONDS)));
        /**
         * 下面两行含义相同
         */
        // env.getCheckpointConfig().setCheckpointInterval(5_000L);
        env.enableCheckpointing(5_000L);

        try {
            // 设置状态后端
            String checkpointPath = "file:///Users/allen/bigdataapp/flinktutorial/temp";
            StateBackend backend = new RocksDBStateBackend(checkpointPath);
            env.setStateBackend(backend);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        env.getConfig().setAutoWatermarkInterval(10_000L);
        DataStream<SensorReading> originSource = env.addSource(new SensorSource());

        DataStream<SensorReading> sensorData = originSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((sensorReading, timestamp) -> {
                    return sensorReading.getTimestamp();
                }));

        KeyedStream<SensorReading, String> keyedSensorData = sensorData.keyBy(value -> value.getId());

        DataStream<Tuple3<String, Long, Long>> alerts = keyedSensorData
                .flatMap(new CheckpointFunctionHighTempCounter(20.0))
                .uid("flatMap001")  // 设置算子标志
                .setMaxParallelism(128);   // 设置最大并行度

        alerts.print();

        try {
            env.execute("Generate Temperature Alerts");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

class CheckpointFunctionHighTempCounter
        implements FlatMapFunction<SensorReading, Tuple3<String, Long, Long>>,
        CheckpointedFunction,
        CheckpointListener {

    private Double threshold;
    // 初始化本地存储当前算子实例高温元素的数量
    private Long opHighTempCnt = 0L;
    // 算子状态，    使用状态列表，  存储当前算子实例高温元素的数量
    private ListState<Long> opCntState;
    // 键值分区状态，    使用单值状态 存储当前元素的键值对应高温元素的数量
    private ValueState<Long> keyedCntState;

    public CheckpointFunctionHighTempCounter(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void flatMap(SensorReading value, Collector<Tuple3<String, Long, Long>> out) throws Exception {
        if (value.getTemperature() > threshold) {
            opHighTempCnt += 1;

            /**
             * org.apache.flink.api.common.state.ValueState#value()
             *
             * Returns the current value for the state. When the state is not
             * partitioned the returned value is the same for all inputs in a given
             * operator instance. If state partitioning is applied, the value returned
             * depends on the current operator input, as the operator maintains an
             * independent state for each partition.
             * 如果状态没有被分区过，对于所有的输入 value(）返回的值是相同的。
             * 如果状态被分区过，value() 返回的值依赖于算子的输入，因为算子为每个键值分区维护一个独立的状态。
             *
             * <p>If you didn't specify a default value when creating the {@link ValueStateDescriptor}
             * this will return {@code null} when to value was previously set using {@link #update(Object)}.
             *
             * @return The state value corresponding to the current input.
             *
             * @throws IOException Thrown if the system cannot access the state.
             */
            Long keyedCnt = keyedCntState.value() + 1;
            keyedCntState.update(keyedCnt);

            out.collect(new Tuple3<>(value.getId(), keyedCnt, opHighTempCnt));
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        opCntState.clear();
        opCntState.add(opHighTempCnt);
        System.out.println("SnapshotState has been involved");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化算子状态     当前算子实例分得的 cnt 状态列表   getOperatorStateStore  ，
        // 获取任务启动时，当前算子任务获取到的列表状态
        opCntState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<Long>("opCnt", TypeInformation.of(Long.class)));
        /**
         * org.apache.flink.api.common.state.AppendingState#get()
         *
         * Returns the current value for the state. When the state is not
         * partitioned the returned value is the same for all inputs in a given
         * operator instance. If state partitioning is applied, the value returned
         * depends on the current operator input, as the operator maintains an
         * independent state for each partition.
         *
         * <p><b>NOTE TO IMPLEMENTERS:</b> if the state is empty, then this method
         * should return {@code null}.
         *
         * @return The operator state value corresponding to the current input or {@code null}
         * if the state is empty.
         *
         * @throws Exception Thrown if the system cannot access the state.
         */
        Iterator<Long> opCntIterator = opCntState.get().iterator();
        while (opCntIterator.hasNext()) {
            // 初始化本地存储的高温元素的数量
            opHighTempCnt += opCntIterator.next();
        }

        // 初始化键值分区状态   getKeyedStateStore
        keyedCntState = context.getKeyedStateStore().getState(
                new ValueStateDescriptor<Long>("keyedCnt", TypeInformation.of(Long.class), 0L));

        System.out.println("InitializeState has been involved");
    }

    /**
     * CheckpointListener 接口中的方法使用示例
     *
     * @param checkpointId
     * @throws Exception
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println("Create checkpoint success, checkpoint id:  " + checkpointId);
    }
}
