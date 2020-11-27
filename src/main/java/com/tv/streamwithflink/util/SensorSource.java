package com.tv.streamwithflink.util;

import com.tv.streamwithflink.bean.SensorReading;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.*;

/**
 * @Description
 * @Author Allen
 * @Date 2020-11-25 14:19
 **/
public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    // flag indicating whether source is still running
    private Boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {

        Integer taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        List<Tuple2<String, Double>> curFTempList = Lists.newArrayList();

        // initialize sensor ids and temperatures
        for(int i =1; i< 6; i++){
            Tuple2<String, Double> curFTempItem = new Tuple2<>("sensor_"+(taskIdx*10+i), 65 + (random.nextGaussian()*20));
            curFTempList.add(curFTempItem);
        }

        while(running){

            // update temperature
            for(Tuple2<String, Double>  item : curFTempList){
                item.setField(item.f1+random.nextGaussian()*0.5, 1);
            }
            // get current time
            Long cur = Calendar.getInstance().getTimeInMillis();
            curFTempList.forEach(t -> ctx.collect(new SensorReading(t.f0, cur, t.f1)));

            Thread.sleep(800);
        }
    }

    /**
     * Cancel this SourceFunction
     */
    @Override
    public void cancel() {
        running = false;
    }

}
