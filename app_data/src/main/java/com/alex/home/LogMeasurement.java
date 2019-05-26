package com.alex.home;

import java.io.Serializable;

public class LogMeasurement implements Serializable {
    private long count;
    private double rate;

    public LogMeasurement() {
    }

    public LogMeasurement(long count, double rate) {
        this.count = count;
        this.rate = rate;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }
}
