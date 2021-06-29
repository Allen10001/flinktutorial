package com.tv.streamwithflink;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.util.SensorSource;
import com.tv.streamwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @Description   时间窗口内气温平均值统计
 * @Author Allen
 * @Date 2020-11-25 11:39
 **/
public class AverageSensorReadings {

    private static final Logger logger = LoggerFactory.getLogger(AverageSensorReadings.class);

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
        })
        // group readings in 1 second windows
        .timeWindow(Time.seconds(1))
        .apply(
                new WindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
            @Override
            public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
                int count = 0;
                Double sum = 0.0;

                Iterator<SensorReading> iterator = input.iterator();
                while(iterator.hasNext()){
                    sum += iterator.next().getTemperature();
                    count++;
                }
                out.collect(new SensorReading(sensorId, window.getEnd(), sum/count));
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
