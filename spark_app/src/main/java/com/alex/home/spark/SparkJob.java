package com.alex.home.spark;

import com.alex.home.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;


public class SparkJob {
    private static String SPARK_APP_NAME = "alex-spark";

    public static void main(String[] args) throws InterruptedException {
        String sparkMasterUrl = getEnv("SPARK_MASTER_URL", "spark://localhost:7077");
        String kafkaBrokerServer = getEnv("KAFKA_BROKER_SERVER", "localhost:9092");
        String inputKafkaTopic = getEnv("INPUT_KAFKA_TOPIC", "input2");
        String outputKafkaTopic = getEnv("OUT_KAFKA_TOPIC", "output");
        String alertsKafkaTopic = getEnv("ALERT_KAFKA_TOPIC", "alerts3");
        Duration windowDuration = Durations.seconds(getIntEnv("WINDOW_DURATION_SEC", 60));
        Duration slideDuration = Durations.seconds(getIntEnv("SLIDE_DURATION_SEC", 10));
        Double alertErrorRateTreshold = getDoubleEnv("ALERT_ERROR_RATE_THERSHOLD", 1.0);

        SparkConf sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(sparkMasterUrl);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, slideDuration);

        Map<String, Object> kafkaParams = KafkaClientUtils.getConsumerProperties(
                kafkaBrokerServer,
                StringDeserializer.class,
                JsonDeserializer.class);

        Collection<String> topics = Arrays.asList(inputKafkaTopic);

        JavaInputDStream<ConsumerRecord<String, LogMessage>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );


        JavaDStream<LogMessageItem> windowStream = stream
                .flatMap(s -> s.value().getItems().iterator())
                .window(windowDuration, slideDuration);

        JavaPairDStream<LogInfoKey, LogMeasurement> aggregatedLogs =
                StreamProcessor.aggregateLogs(windowStream, windowDuration.milliseconds());

        aggregatedLogs.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                Producer<LogInfoKey, LogMeasurement> producer =
                        KafkaClientUtils.createProducer(kafkaBrokerServer, JsonSerializer.class, JsonSerializer.class);
                partition.forEachRemaining(info -> {
                    producer.send(new ProducerRecord<>(
                            outputKafkaTopic,
                            info._1,
                            info._2)
                    );
                });
            });
        });

        JavaDStream<ErrorAlert> errorAggregated =
                StreamProcessor.aggregateErrorLogs(windowStream, windowDuration.milliseconds(), alertErrorRateTreshold);


        errorAggregated.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                Producer<String, Double> producer =
                        KafkaClientUtils.createProducer(kafkaBrokerServer, StringSerializer.class, DoubleSerializer.class);
                partition.forEachRemaining(info -> {
                    producer.send(new ProducerRecord<>(
                            alertsKafkaTopic,
                            info.getHost(),
                            info.getError_rate())
                    );
                });
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null ? defaultValue : value;
    }

    private static int getIntEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static double getDoubleEnv(String name, double defaultValue) {
        String value = System.getenv(name);
        return value == null ? defaultValue : Double.parseDouble(value);
    }
}
