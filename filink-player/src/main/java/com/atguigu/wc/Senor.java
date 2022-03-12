package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.TypeInfo;


public class Senor {
    private String id;
    private Double temp;
    private Long currTime;

    public Senor(String id, Double temp, Long currTime) {
        this.id = id;
        this.temp = temp;
        this.currTime = currTime;
    }

    public Senor() {
    }

    @Override
    public String toString() {
        return "Senor{" +
                "id='" + id + '\'' +
                ", temp=" + temp +
                ", currTime=" + currTime +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getTemp() {
        return temp;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    public Long getCurrTime() {
        return currTime;
    }

    public void setCurrTime(Long currTime) {
        this.currTime = currTime;
    }
}
