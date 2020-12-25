package com.tv.streamwithflink.flinkstate;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.util.SensorSource;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;

import java.time.Duration;
import java.util.List;

/**
 * @Description 1、通过 ListCheckpointed 接口实现算子列表状态
 * 2、统计温度超过阈值的个数
 * @Author Allen
 * @Date 2020-12-22 15:26
 **/
public class ListCheckpointedFunction007 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointInterval(5 * 1000);
        env.getConfig().setAutoWatermarkInterval(10_000);
        env.setParallelism(8);
        DataStream<SensorReading> originSource = env.addSource(new SensorSource());

        DataStream<SensorReading> sensorData = originSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((sensorReading, timestamp) -> {
                    return sensorReading.getTimestamp();
                }));

        KeyedStream keyedSensorData = sensorData.keyBy(value -> value.getId());

        DataStream<Tuple2<Integer, Long>> alerts = keyedSensorData
                .flatMap(new HighTempCounter(50.0));

        alerts.print();

        try {
            env.execute("Generate Temperature Alerts");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }


    }
}

// 统计温度超过阈值的个数
class HighTempCounter extends RichFlatMapFunction<SensorReading, Tuple2<Integer, Long>> implements ListCheckpointed<Long> {

    // 阈值
    private Double threshold;
    // 超过阈值温度的数量
    private Long highTempCount;
    // 子任务索引号
    private Integer subTaskIndex;

    public HighTempCounter(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.highTempCount = 0L;
        // 必须在 open 中调用 getRuntimeContext 才能获取非空的 RuntimeContext
        this.subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void flatMap(SensorReading value, Collector<Tuple2<Integer, Long>> out) throws Exception {

        if (value.getTemperature() > threshold) {
            highTempCount++;
            out.collect(new Tuple2<>(subTaskIndex, highTempCount));
        }
    }

    // 以列表形式返回一个函数状态的快照
    /*@Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(highTempCount);
    }*/

    /**
     * 算子列表拆分, 从而在扩缩容时实现更佳的分布
     */
    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
        Long div = highTempCount / 10;
        int mod = Integer.parseInt((Long.toString(highTempCount % 10L)));
        List<Long> res = Lists.newArrayList();
        for (int i = 0; i < mod; i++) {
            res.add(div + 1);
        }
        for (int j = mod; j < 10; j++) {
            res.add(div);
        }
        return res;
    }

    /**
     *  根据提供的列表恢复函数状态
      */
    @Override
    public void restoreState(List<Long> state) throws Exception {
        highTempCount = 0L;
        for (Long item : state) {
            highTempCount += item;
        }
        return;
    }
}
