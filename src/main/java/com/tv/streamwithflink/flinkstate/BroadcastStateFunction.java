package com.tv.streamwithflink.flinkstate;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.bean.ThresholdUpdateBean;
import com.tv.streamwithflink.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.io.Serializable;
import java.time.Duration;

/**
 * @Description 使用联结的广播状态  &  serializable test
 * @Author Allen
 * @Date 2020-12-22 20:35
 **/
public class BroadcastStateFunction {

    public static final String serializableTest002Value = "serializableTest002";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.getCheckpointConfig().setCheckpointInterval(10_000L);

        DataStream<SensorReading> originSource = env.addSource(new SensorSource());
//      System.out.println("parallelism: "+originSource.getParallelism());  // 4

        DataStream<SensorReading> sensorData = originSource.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((sensorReading, timestamp) -> {
                    return sensorReading.getTimestamp();
                }));


        KeyedStream<SensorReading, String> keyedSensorData = sensorData.keyBy(value -> value.getId());

        DataStream<ThresholdUpdateBean> thresholds = env.fromElements(
                new ThresholdUpdateBean("sensor_1", 5.0d),
                new ThresholdUpdateBean("sensor_2", 0.9d),
                new ThresholdUpdateBean("sensor_3", 0.5d),
                new ThresholdUpdateBean("sensor_1", 1.2d),    // update threshold for sensor_1
                new ThresholdUpdateBean("sensor_3", 0.0d)     // disable threshold for sensor_3
        );

        MapStateDescriptor<String, Double> broadcastStateDescriptor =
                new MapStateDescriptor<String, Double>("threshold",
                        TypeInformation.of(String.class),
                        TypeInformation.of(Double.TYPE));

        /**
         * thresholds.broadcast(final MapStateDescriptor<?, ?>... broadcastStateDescriptors)
         *
         * Sets the partitioning of the {@link DataStream} so that the output elements
         * are broadcasted to every parallel instance of the next operation. In addition,
         * it implicitly creates as many {@link org.apache.flink.api.common.state.BroadcastState broadcast states}
         * as the specified descriptors which can be used to store the element of the stream.
         * 隐式创建和 MapStateDescriptor 状态描述符一样多的 BroadcastState，存储流的元素
         * @param broadcastStateDescriptors the descriptors of the broadcast states to create.
         * @return A {@link BroadcastStream} which can be used in the {@link #connect(BroadcastStream)} to
         * create a {@link BroadcastConnectedStream} for further processing of the elements.
         */
        BroadcastStream<ThresholdUpdateBean> broadcastStream = thresholds.broadcast(broadcastStateDescriptor);


        UpdateTempAlertsFunction updateTempAlertsFunction = new UpdateTempAlertsFunction(); // serializable test
//        updateTempAlertsFunction.setSerializableTest002(serializableTest002Value); // serializable test

        DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData
                .connect(broadcastStream)
                .process(updateTempAlertsFunction);

        // serializable test, updateTempAlertsFunction 对象传递给 process() 方法之后，
        // 再set value，也可以 set value 成功，原因可能是因为 env.execute（） 之前，并没有触发task计算，
        // 所以 process 方法中传递形参 updateTempAlertsFunction 时的序列化对象也并没有传递给 TM,
        // 最终传递给 TM 的是 env.execute（）之前对 updateTempAlertsFunction 对象进行的所有初始化操作完成后
        // 的updateTempAlertsFunction对象
        updateTempAlertsFunction.setSerializableTest002(serializableTest002Value);

        alerts.print();

        try {
            env.execute("Generate Temperature Alerts According to Updating Threshold");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

/**
 * the functions need to be serializable: The function may not contain any non-serializable fields,
 * i.e. types that are not primitive (int, long, double, ...) and not implementing java.io.Serializable.
 */
class UpdateTempAlertsFunction extends KeyedBroadcastProcessFunction<String, SensorReading, ThresholdUpdateBean, Tuple3<String, Double, Double>> {

    private MapStateDescriptor<String, Double> updateThresholdsDescriptor =
            new MapStateDescriptor<>(
                    "threshold",
                    TypeInformation.of(String.class),
                    TypeInformation.of(Double.class));
    private ValueStateDescriptor<Double> lastTempStateDescriptor =
            new ValueStateDescriptor<>(
                    "lastTemp",
                    TypeInformation.of(Double.class));

    // ValueState 没有实现序列化接口，需要在open方法中初始化
    // https://stackoverflow.com/questions/34118469/flink-using-dagger-injections-not-serializable
    private ValueState<Double> lastTempState;
    private String serializableTest001;
    private String serializableTest002;

    public UpdateTempAlertsFunction() {
        System.out.println("serializableTest001 in constructor: "+serializableTest001);
        System.out.println("serializableTest002 in constructor: "+serializableTest002);
    }

    public void setSerializableTest002(String serializableTest002) {
        this.serializableTest002 = serializableTest002;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastTempState = getRuntimeContext().getState(lastTempStateDescriptor);
        serializableTest001 = "serializableTest";
    }

    @Override
    public void processElement(SensorReading value, ReadOnlyContext ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {

        System.out.println("serializableTest001: "+serializableTest001);
        System.out.println("serializableTest002: "+serializableTest002);

        ReadOnlyBroadcastState<String, Double> updateThresholdsState = ctx.getBroadcastState(updateThresholdsDescriptor);
        if (lastTempState != null && updateThresholdsState.contains(value.getId())) {
            Double lastTemp = lastTempState.value();
            Double diff = Math.abs(value.getTemperature() - lastTemp);
            if (diff > updateThresholdsState.get(value.getId())) {
                out.collect(new Tuple3<>(value.getId(), value.getTemperature(), diff));
            }
        }
        lastTempState.update(value.getTemperature());
    }

    @Override
    public void processBroadcastElement(ThresholdUpdateBean value, Context ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        BroadcastState<String, Double> updateBeanMapState = ctx.getBroadcastState(updateThresholdsDescriptor);
        if (value.getThreshold() != 0.0d) {
            // configure a new threshold for the sensor
            updateBeanMapState.put(value.getId(), value.getThreshold());
        } else {
            //  disable threshold for value.getId()
            updateBeanMapState.remove(value.getId());
        }
    }
}
