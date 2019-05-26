package com.alex.home;

import java.util.Arrays;
import java.util.List;

public class LogLevel {
    public static final String TRACE = "TRACE";
    public static final String DEBUG = "DEBUG";
    public static final String INFO = "INFO";
    public static final String WARN = "WARN";
    public static final String ERROR = "ERROR";

    private static final List<String> LEVELS = Arrays.asList(TRACE, DEBUG, INFO, WARN, ERROR);

    public static List<String> getLevels() {
        return LEVELS;
    }
}
