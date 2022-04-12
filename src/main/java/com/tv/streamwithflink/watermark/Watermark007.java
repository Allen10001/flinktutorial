package com.tv.streamwithflink.watermark;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Description
 * @Author Allen
 * @Date 2020-12-09 11:38
 **/
public class Watermark007 {

    public static void main(String[] args) {
        if(args.length != 2){
            System.err.println("USAGE:\\Watermark007 <hostname> <port>");
            System.exit(1);
        }

        String hostName = args[0];
        Integer port = Integer.parseInt(args[1]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 时间特性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 水位线间隔
        env.getConfig().setAutoWatermarkInterval(15_000L);

        DataStream<String> originStream = env.socketTextStream(hostName, port);

        DataStream<Tuple2<String, Long>> streamWithKeyAndTime = originStream
                .map(item -> {
            String[] arr = item.split("\\|");
            if(arr.length != 2){
                System.err.println(item+" is not allowed，USAGE: CODE|TIMESTAMP");
            }
            String code = arr[0];
            Long timestamp = Long.parseLong(arr[1]);
            return Tuple2.of(code, timestamp);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        DataStream<Tuple2<String, Long>> afterAssignDataStream = streamWithKeyAndTime
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 返回一个泛型类 WatermarkStrategy<Tuple2<String, Long>>， static的方法，泛型在点的后面
                                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                // 调用泛型类 WatermarkStrategy<Tuple2<String, Long>> 的withTimestampAssigner() 方法
                                .withTimestampAssigner((event, timestamp) -> event.f1));

        DataStream<Tuple2<String, byte[]>> dataStream = afterAssignDataStream.map(
            new MapFunction<Tuple2<String, Long>, Tuple2<String, byte[]>>() {
                @Override
                public Tuple2<String, byte[]> map(Tuple2<String, Long> item) throws Exception {
                    return new Tuple2(item.f0, item.f0.getBytes(StandardCharsets.UTF_8));
                }
            });

        KeyedStream<Tuple2<String, byte[]>, String> keyedStream = dataStream.keyBy(new KeySelector<Tuple2<String,byte[]>, String>() {
            @Override
            public String getKey(Tuple2<String, byte[]> value) throws Exception {
                return value.f0;
            }
        });

        //originStream.print();
        keyedStream.print();
       // afterAssignDataStream.print();

        try {
            env.execute("WatermarkDemo");
        }catch (Exception e){
            System.err.println("Watermark Demo Execute Failed!!!");
            System.exit(1);
        }
    }
}
