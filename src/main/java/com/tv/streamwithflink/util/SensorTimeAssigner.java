package com.tv.streamwithflink.util;

import com.tv.streamwithflink.bean.SensorReading;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Description
 * @Author Allen
 * @Date 2020-11-25 16:15
 **/
public class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {

    public SensorTimeAssigner(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(SensorReading element) {
        return element.getTimestamp();
    }
}
