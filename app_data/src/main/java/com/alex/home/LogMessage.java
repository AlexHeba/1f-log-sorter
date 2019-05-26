package com.alex.home;

import java.util.List;

public class LogMessage implements java.io.Serializable {
    private List<LogMessageItem> items;

    public LogMessage() {}

    public LogMessage(List<LogMessageItem> items) {
        this.items = items;
    }

    public List<LogMessageItem> getItems() {
        return items;
    }

    public void setItems(List<LogMessageItem> items) {
        this.items = items;
    }
}
