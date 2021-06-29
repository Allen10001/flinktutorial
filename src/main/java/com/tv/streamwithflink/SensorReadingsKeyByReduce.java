package com.tv.streamwithflink;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.util.SensorSource;
import com.tv.streamwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description 基于键值的reduce, 不会清理状态，需要键值域有限
 * @Author Allen
 * @Date 2020-11-26 17：02
 **/
public class SensorReadingsKeyByReduce {

    private static final Logger logger = LoggerFactory.getLogger(SensorReadingsKeyByReduce.class);

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);
        // assign timestamps and watermarks which are required for event time
        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5L)));

        DataStream<SensorReading> avgSensorData = sensorData
        // convert Fahrenheit to Celsius using an inlined map function
        .map(item -> {
            item.setTemperature((item.getTemperature()-32) * (5.0 / 9.0));
            return item;
        })
        // organize stream by sensorId
        .keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        }).reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        return new SensorReading(value1.getId(),
                                (value1.getTimestamp()+value2.getTimestamp())/2,
                                (value1.getTemperature()+value2.getTemperature())/2);
                    }
                });

        avgSensorData.print();

        try {
            env.execute("Compute Average Sensor Temperature");
        }catch (Exception e){
            logger.error("Execute task failed! ");
            System.exit(1);
        }
    }
}


