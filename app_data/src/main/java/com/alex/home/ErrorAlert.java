package com.alex.home;

import java.io.Serializable;

public class ErrorAlert implements Serializable {
    private String host;
    private Double error_rate;

    public ErrorAlert() {
    }

    public ErrorAlert(String host, Double error_rate) {
        this.host = host;
        this.error_rate = error_rate;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Double getError_rate() {
        return error_rate;
    }

    public void setError_rate(Double error_rate) {
        this.error_rate = error_rate;
    }
}
