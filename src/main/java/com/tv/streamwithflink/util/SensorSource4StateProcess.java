package com.tv.streamwithflink.util;

import com.tv.streamwithflink.bean.SensorReading;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * @Description
 * @Author Allen
 * @Date 2020-11-25 14:19
 **/
public class SensorSource4StateProcess extends RichParallelSourceFunction<SensorReading> {

    // flag indicating whether source is still running
    private Boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {

        Integer taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        List<Tuple2<String, Double>> curFTempList = Lists.newArrayList();

        // initialize sensor ids and temperatures
        for(int i =1; i< 3; i++){
            Tuple2<String, Double> curFTempItem = new Tuple2<>("sensor_"+(taskIdx*10+i), 65 + (random.nextGaussian()*20));
            curFTempList.add(curFTempItem);
        }

        while(running){

            // update temperature
            for(Tuple2<String, Double>  item : curFTempList){
                item.setField(item.f1+random.nextGaussian()*0.5, 1);
            }

            curFTempList.forEach(t -> {
                    // Calendar.getInstance().getTimeInMillis()  get current time, 记录的时间戳为当前时间
                    ctx.collect(new SensorReading(t.f0, Calendar.getInstance().getTimeInMillis(), t.f1));
                    // 每隔一段时间输入一条记录， 在测试计时器触发时间时，可以将该值设大一点 ，如 10_000，平时可以保持 100ms
                    try {
                        Thread.sleep(10_000);
                        //Thread.sleep(100);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    }
                );


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
