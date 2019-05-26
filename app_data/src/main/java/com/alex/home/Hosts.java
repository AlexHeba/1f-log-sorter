package com.alex.home;

import java.util.Arrays;
import java.util.List;

public class Hosts {
    private static final List<String> HOSTS = Arrays.asList(
            "host1",
            "host2",
            "host3",
            "host4"
    );

    public static List<String> getHosts() {
        return HOSTS;
    }
}
