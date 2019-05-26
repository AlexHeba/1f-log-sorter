package com.alex.home;

public class ErrorAlert {
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
