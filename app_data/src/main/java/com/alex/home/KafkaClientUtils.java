package com.alex.home;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaClientUtils {
    public static <K, V> Producer<K, V> createProducer(String bootstrapServer, Class keySerializer, Class valueSerializer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", keySerializer);
        props.put("value.serializer", valueSerializer);
        return new KafkaProducer<K, V>(props);
    }

    public static Map<String, Object> getConsumerProperties(String bootstrapServer, Class keyDeserializer, Class valueDeserializer) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", bootstrapServer);
        kafkaParams.put("key.deserializer", keyDeserializer);
        kafkaParams.put("value.deserializer", valueDeserializer);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put(JsonDeserializer.TRUSTED_PACKAGES, "com.alex.home");
        return kafkaParams;
    }
}
