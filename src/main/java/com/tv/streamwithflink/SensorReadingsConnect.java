package com.tv.streamwithflink;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.util.SensorSource;
import com.tv.streamwithflink.util.SensorTimeAssigner;
import com.tv.streamwithflink.util.ThresholdsSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @Description 1. 合并两条流, 找出哪个传感器现在温度大于阈值，并标记为 危险
 * 2. Split 和 Select
 * 3. 其他的api
 * @Author Allen
 * @Date 2020-11-26 17：02
 **/
public class SensorReadingsConnect {

    private static final Logger logger = LoggerFactory.getLogger(SensorReadingsConnect.class);

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // Get parallelism
        System.out.println("======"+env.getParallelism());

        // assign timestamps and watermarks which are required for event time
        DataStream<SensorReading> sensorData = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5L)));

        // 温度阈值流, 温度单位 Celsius
        DataStream<SensorReading> thresholds = env.addSource(new ThresholdsSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5L)));

        ConnectedStreams<SensorReading, SensorReading> conSensorDataWithThresholds = sensorData
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
          // 连接两个按键值分区的流
         .connect(thresholds.keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading value) throws Exception {
                        return value.getId();
                    }
                }));

        DataStream<Tuple2<SensorReading, String>> dangerTemperatureDataStream = conSensorDataWithThresholds.flatMap(
                new CoFlatMapFunction<SensorReading, SensorReading, Tuple2<SensorReading, String>>(){

            Double thresholdsTemperature;

            @Override
            public void flatMap1(SensorReading value, Collector<Tuple2<SensorReading, String>> out) throws Exception {
                if(thresholdsTemperature !=null && value.getTemperature() > thresholdsTemperature) {
                    out.collect(new Tuple2<SensorReading, String>(value, "danger"));
                }
            }

            @Override
            public void flatMap2(SensorReading value, Collector<Tuple2<SensorReading, String>> out) throws Exception {
                thresholdsTemperature = value.getTemperature();
            }
        });

        /**
         * 合并两条流, 找出哪个传感器现在温度大于阈值，并标记为 危险  , 打印
         */
        //dangerTemperatureDataStream.print();

        /**
         * Split 和 Select
         */
        SplitStream<SensorReading> sensorReadingSplitStream = sensorData.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                if(value.getTemperature()< 40.0){
                    return Arrays.asList(new String[]{"cold", "low"});
                }else{
                    return Arrays.asList(new String[]{"hot", "high"});
                }
            }
        });

        // 参数的值、数量不变，顺序可变
        // sensorReadingSplitStream.select("hot", "high").print();       // 可以
        //sensorReadingSplitStream.select("low", "cold").print();   // 可以
        // sensorReadingSplitStream.select("cold").print();   // 不行

        /**
         * 分发转换
         */
/*

        // 随机
        sensorData.shuffle();
        // 重调
        sensorData.rescale();
        // 广播
        sensorData.broadcast();
        // 轮流
        sensorData.rebalance();
        // 全局
        sensorData.global();
*/

        // 创建类型信息
        /*
        TypeInformation<Integer> intType= Types.INT;
        TypeInformation<Tuple2<Long, String>> tupleType = Types.TUPLE(Types.LONG, Types.STRING);
        TypeInformation<SensorReading> personType = Types.POJO(SensorReading.class);
        */
        // 指定算子返回类型
        sensorData.map(item -> item).returns(Types.POJO(SensorReading.class));
        sensorData.print();

        try {
            env.execute("Compute Danger Sensor Temperature");
        }catch (Exception e){
            logger.error("Execute task failed! ");
            System.exit(1);
        }
    }
}


