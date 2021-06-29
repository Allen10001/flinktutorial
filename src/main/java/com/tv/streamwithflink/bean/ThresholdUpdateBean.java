package com.tv.streamwithflink.bean;

/**
 * @Description
 * @Author Allen
 * @Date 2020-12-22 20:39
 **/
public class ThresholdUpdateBean {

    private String id;
    private Double threshold;

    public ThresholdUpdateBean(String id, Double threshold) {
        this.id = id;
        this.threshold = threshold;
    }

    public ThresholdUpdateBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getThreshold() {
        return threshold;
    }

    public void setThreshold(Double threshold) {
        this.threshold = threshold;
    }

}
