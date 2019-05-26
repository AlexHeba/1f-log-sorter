package com.alex.home.spark;

import com.alex.home.LogMessageItem;

public class JavaRow implements java.io.Serializable {
    private LogMessageItem word;

    public LogMessageItem getWord() {
        return word;
    }

    public void setWord(LogMessageItem word) {
        this.word = word;
    }
}
