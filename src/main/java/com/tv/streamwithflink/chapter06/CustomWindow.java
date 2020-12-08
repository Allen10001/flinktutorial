package com.tv.streamwithflink.chapter06;

import com.tv.streamwithflink.bean.SensorReading;
import com.tv.streamwithflink.util.SensorSource;
import com.tv.streamwithflink.util.SensorTimeAssigner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * @Description
 * @Author Allen
 * @Date 2020-12-07 15:25
 **/
public class CustomWindow {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // todo checkpoint every 10 seconds  ???
        env.getCheckpointConfig().setCheckpointInterval(10_000);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(1);

        DataStream<SensorReading> sensorData = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5L)));

        KeyedStream<SensorReading, String> keyedStream = sensorData.keyBy(t->t.getId());
        //keyedStream.print();
        WindowedStream<SensorReading, String, TimeWindow> windowedStream = keyedStream
                .window(new ThirtySecondsWindows())
                .trigger(new OneSecondIntervalTrigger());

        DataStream<Tuple5<String,Long, Long, Long, Integer>> dataStreamAfterProcess =
                windowedStream
                        // count readings per window
                        .process(new SensorDataProcessWindowFunction());

        dataStreamAfterProcess.print();

        try{
            env.execute("Count Window Elements Number Every 1 Second");
        }catch (Exception e){
            System.out.println("count window elements number every 1 second");
            System.exit(1);
        }
    }


    /**
     * A custom window that groups events in to 30 second tumbling windows.
     */
    public static class ThirtySecondsWindows extends WindowAssigner<Object, TimeWindow>{

        long windowSize = 30_000L;

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
            long startTime = timestamp - (timestamp % windowSize);
            long endTime = startTime + windowSize;
            return Collections.singletonList(new TimeWindow(startTime, endTime));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }
    }

    /**
     * A trigger that fires early. The trigger fires at most every second.
     */
    public static class OneSecondIntervalTrigger extends Trigger<SensorReading, TimeWindow>{

        @Override
        public TriggerResult onElement(SensorReading element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

            // firstSeen will be false if not set yet
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(
                    new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));

            if (firstSeen.value() == null || !firstSeen.value()) {
                long temp = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                ctx.registerEventTimeTimer(temp);
                ctx.registerEventTimeTimer(window.getEnd());
                firstSeen.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // Continue. We don't use processing time timers
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if(time == window.getEnd()){
                // final evaluation and purge window state
                return TriggerResult.FIRE_AND_PURGE;
            }else{
                // register next early firing timer
                long temp = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                if(temp < window.getEnd()){
                    ctx.registerEventTimeTimer(temp);
                }
                // fire trigger to early evaluate window
                return TriggerResult.FIRE;
            }
        }

        /**
         * Clears any state that the trigger might still hold for the given window. This is called
         * when a window is purged.
         */
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

            // clear trigger state
            ValueState<Boolean> valueState = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));
            valueState.clear();
        }
    }

    public static class SensorDataProcessWindowFunction extends
            ProcessWindowFunction<SensorReading, Tuple5<String, Long, Long, Long, Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<SensorReading> elements,
                            Collector<Tuple5<String, Long, Long, Long, Integer>> out) throws Exception {

            int count = 0;
            Iterator iterator = elements.iterator();
            while(iterator.hasNext()){
                count++;
                iterator.next();
            }
         /*   for (SensorReading r : elements) {
                count++;
            }*/
            // get current watermark
            long evalTime = context.currentWatermark();
            // emit results
            out.collect(Tuple5.of(key, context.window().getStart(), context.window().getEnd(), evalTime, count));
        }
    }
}


