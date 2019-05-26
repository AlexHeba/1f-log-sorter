package com.alex.home;

import java.sql.Timestamp;
import java.util.Date;

public class LogMessageItem implements java.io.Serializable {
    private String timesatmp;
    private String host;
    private String level;
    private String text;

    public LogMessageItem() {}

    public LogMessageItem(String host, String level, String text) {

        this.timesatmp = new Timestamp(new Date().getTime()).toString();
        this.host = host;
        this.level = level;
        this.text = text;
    }

    public String getTimesatmp() {
        return timesatmp;
    }

    public void setTimesatmp(String timesatmp) {
        this.timesatmp = timesatmp;
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

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
