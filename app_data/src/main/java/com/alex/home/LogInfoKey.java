package com.alex.home;

import java.io.Serializable;

public class LogInfoKey implements Serializable {
    private String host;
    private String level;

    public LogInfoKey() {}

    public LogInfoKey(String host, String level) {
        this.host = host;
        this.level = level;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }
}
