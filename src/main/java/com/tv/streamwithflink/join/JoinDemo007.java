package com.tv.streamwithflink.join;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author Allen
 **/
public class JoinDemo007 {

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

        // 1. 基于时间间隔的 join
        // ProcessJoinFunction
        keyedStream.intervalJoin(keyedStream)
            .between(Time.hours(-1), Time.hours(1))
            .process(
            new ProcessJoinFunction<Tuple2<String,byte[]>, Tuple2<String,byte[]>, Object>() {

                @Override
                public void processElement(Tuple2<String, byte[]> left,
                    Tuple2<String, byte[]> right, Context ctx, Collector<Object> out)
                    throws Exception {

                }
            });

        // 2. 基于窗口的join
        /**
         * 顾名思义，基于窗口的Join需要用到Flink中的窗口机制。
         * 其原理是将两条输入流中的元素分配到公共窗口中并在窗口完成时进行Join（或Cogroup）。
         * 除了对窗口中的两条流进行Join，你还可以对它们进行Cogroup，只需将算子定义开始位置的join改为coGroup()即可。
         * Join和Cogroup的总体逻辑相同，二者的唯一区别是：Join会为两侧输入中的每个 事件对 调用JoinFunction；
         * 而Cogroup中用到的CoGroupFunction会以两个输入的元素遍历器为参数，只在每个窗口中被调用一次。
         */
        keyedStream.join(keyedStream)
            .where(value -> value.f0)
            .equalTo(value -> String.valueOf(value.f1))
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
             // 当窗口的计时器触发时，算子会遍历两个输入中元素的每个组合（叉乘积）去调用JoinFunction
            .apply(new JoinFunction<Tuple2<String,byte[]>, Tuple2<String,byte[]>, Object>() {
                @Override
                public Object join(Tuple2<String, byte[]> first, Tuple2<String, byte[]> second)
                    throws Exception {
                    return null;
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
