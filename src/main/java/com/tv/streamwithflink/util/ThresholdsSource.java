package com.tv.streamwithflink.util;

import com.tv.streamwithflink.bean.SensorReading;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * @Description 温度阈值流
 * @Author Allen
 * @Date 2020-11-26 18:35
 **/
public class ThresholdsSource extends RichParallelSourceFunction<SensorReading> {

    private Boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Integer taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        List<String> ids = Lists.newArrayList();
        for(int i=1; i < 6; i++){
            ids.add("sensor_"+(taskIdx*10+i));
        }

        // Double[] thresholds = {10.0, 20.0, 30.0, 40.0, 50.0};
        while(running){
            for(int i=1; i<6; i++){
                ctx.collect(new SensorReading(ids.get(i-1), Calendar.getInstance().getTimeInMillis(), 25.0));
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
