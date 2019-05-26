package com.alex.home.kafka;

import com.alex.home.Hosts;
import com.alex.home.LogLevel;
import com.alex.home.LogMessage;
import com.alex.home.LogMessageItem;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;

public class MessageGenerator {

    private static Random RANDOMIZER = new Random();

    public static Pair<String, LogMessage> generate(int count, String level) {
        List<LogMessageItem> items = new ArrayList<>(count);
        Function<String, String> getLevel = level == null ? v -> getRandomHost() : v -> level;
        for (int i = 0; i < count; i++) {
            items.add(new LogMessageItem(
                    getRandomHost(),
                    getLevel.apply(""),
                    getRandomText()
            ));
        }

        return new Pair<>(
                UUID.randomUUID().toString(),
                new LogMessage(items));
    }

    private static String getRandomText() {
        return UUID.randomUUID().toString();
    }

    private static String getRandomLevel() {
        return LogLevel.getLevels().get(RANDOMIZER.nextInt(LogLevel.getLevels().size()));
    }

    private static String getRandomHost() {
        return Hosts.getHosts().get(RANDOMIZER.nextInt(Hosts.getHosts().size()));
    }
}
