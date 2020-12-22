package com.tv.streamwithflink.flinkstate;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.time.Duration;

/**
 * @Description 检测温度测量值的变化是否超过了阈值
 * @Author Allen
 * @Date 2020-12-21 18:16
 **/
public class KeyedStateFunction {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.getCheckpointConfig().setCheckpointInterval(10_000L);
        env.setParallelism(1);
        DataStream<SensorReading> originSource = env.addSource(new SensorSource());

        DataStream<SensorReading> sensorData = originSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((sensorReading, timestamp)-> {
                    return sensorReading.getTimestamp();
                }));

        KeyedStream keyedSensorData = sensorData.keyBy(value -> "_");
        // KeyedStream keyedSensorData = sensorData.keyBy(value -> value.getId());
        DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData
                .flatMap(new TemperatureAlertFunction(5.0));

        alerts.print();

        try {
            env.execute("Generate Temperature Alerts");
        }catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }

    }

}

class TemperatureAlertFunction extends RichFlatMapFunction<SensorReading ,Tuple3<String, Double, Double>>{

    // 创建状态引用对象, 状态引用对象只是提供访问状态的接口，而不会存储状态本身，具体保存工作由状态后端完成
    private ValueState<Double> lastTempState;
    // 阈值
    private Double threshold;
    // 创建状态描述符
    ValueStateDescriptor<Double> lastTempDescriptor;

    public TemperatureAlertFunction(Double threshold) {
        this.threshold = threshold;
        this.lastTempDescriptor =
                new ValueStateDescriptor<>("lastTemp", TypeInformation.of(Double.class));
    }

    /**
     * 只能做赋值操作
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 获取状态对象
        lastTempState = getRuntimeContext().getState(lastTempDescriptor);
    }

    /**The core method of the FlatMapFunction. Takes an element from the input data set and transforms
	 * it into zero, one, or more elements.
     */
    @Override
    public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

        Double lastTamp = 0.0;
        System.out.println("lastTamp="+lastTempState.value());
        System.out.println("currentTamp="+value.getTemperature());

        if(lastTempState != null && null != lastTempState.value()){
            lastTamp = lastTempState.value();
        }

        // 当前温度和上一个元素的温度差
        Double diff = Math.abs(value.getTemperature() - lastTamp);
        System.out.println("diff="+diff.toString());
        if(diff > threshold){
            out.collect(new Tuple3<>(value.getId(), value.getTemperature(), diff));
        }
        // 更新最后一个元素的 temperature
        lastTempState.update(value.getTemperature());
        System.out.println("updated lastTamp="+lastTempState.value());
    }

}
