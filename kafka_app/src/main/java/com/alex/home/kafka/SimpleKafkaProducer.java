package com.alex.home.kafka;

import com.alex.home.LogMessage;
import com.alex.home.KafkaClientUtils;
import javafx.util.Pair;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SimpleKafkaProducer {

    private static final Random RANDOMIZER = new Random();

    public static void main(String[] args) throws Exception {
        int parallelCount = 2;

        if (args.length < 1) {
            throw new IllegalStateException("Kafka topic name must be set");
        }
        String topicName = args[0];

        if (args.length < 2) {
            throw new IllegalStateException("Kafka bootstrap server must be set");
        }
        String server = args[1];

        if (args.length > 2) {
            parallelCount = Integer.parseInt(args[2]);
        }

        String preferLevel = args.length > 3 ? args[3] : null;

        ExecutorService executor = Executors.newFixedThreadPool(parallelCount);
        for (int i = 0; i < parallelCount; i++) {
            executor.execute(() -> ProduceToKafka(server, topicName, preferLevel));
        }
    }

    private static void ProduceToKafka(String server, String topicName, String preferLevel) {
        try (Producer<String, LogMessage> producer = KafkaClientUtils.createProducer(server, StringSerializer.class, JsonSerializer.class)) {
            while (true) {
                Pair<String, LogMessage> record = MessageGenerator.generate(RANDOMIZER.nextInt(9) + 1, preferLevel);
                Future<RecordMetadata> response = producer.send(new ProducerRecord<>(
                        topicName,
                        record.getKey(),
                        record.getValue()));
                RecordMetadata metadata = response.get();
                System.out.println(metadata.toString());

                Thread.sleep((RANDOMIZER.nextInt(10) + 1) * 1000);
            }
        } catch (Exception ex) {
            System.out.println("Error occurred: " + ex.getMessage());
        }
    }
}