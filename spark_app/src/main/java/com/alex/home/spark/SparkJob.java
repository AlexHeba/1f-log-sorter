package com.alex.home.spark;

import com.alex.home.KafkaClientUtils;
import com.alex.home.LogMessage;
import com.alex.home.LogMessageItem;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.apache.spark.sql.expressions.javalang.typed.count;

public class SparkJob {
    public static void main(String[] args) throws InterruptedException {
        String sparkMasterUrl = getEnv("SPARK_MASTER_URL", "spark://alex-vb:7077");
        String kafkaBrokerServer = getEnv("KAFKA_BROKER_SERVER", "localhost:9092");
        String inputKafkaTopic = getEnv("INPUT_KAFKA_TOPIC", "input2");
        String outputKafkaTopic = getEnv("OUT_KAFKA_TOPIC", "output");
        String alertsKafkaTopic = getEnv("ALERT_KAFKA_TOPIC", "alerts3");
        Duration windowDuration = Durations.seconds(getIntEnv("WINDOW_DURATION_SEC", 60));
        Duration slideDuration = Durations.seconds(getIntEnv("SLIDE_DURATION_SEC", 10));
        Double alertErrorRateTreshold = getDoubleEnv("ALERT_ERROR_RATE_THERSHOLD", 1.0);

        SparkConf sparkConf = new SparkConf().setAppName("alex-spark").setMaster(sparkMasterUrl);
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

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        windowStream.foreachRDD(rdd -> {
            Dataset logsDataFrame = spark.createDataFrame(rdd, LogMessageItem.class);

            logsDataFrame.show();

            Dataset aggregatedLogsDataframe =
                    logsDataFrame
                        .groupBy("host", "level")
                        .agg(count(n -> n).name("count"));

            Dataset aggregatedLogsDataframe2 =
                    aggregatedLogsDataframe
                        .withColumn("rate", aggregatedLogsDataframe
                                .col("count")
                                .multiply(1000.0)
                                .divide(windowDuration.milliseconds()))
                        .selectExpr("host", "level", "count", "rate");

            aggregatedLogsDataframe2
                    .selectExpr("CAST(CONCAT(host, ' ', level) AS STRING) as key", "CAST(CONCAT(count, ' ', rate) AS STRING) as value")
                    .write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBrokerServer)
                    .option("topic", outputKafkaTopic)
                    .save();

            Dataset errorAggregatedDataframe =
                    logsDataFrame
                            .filter("level == 'ERROR'")
                            .groupBy("host")
                            .agg(count(n -> n).name("cnt"));

            errorAggregatedDataframe.show();

            Dataset alertsDataframe =
                    errorAggregatedDataframe
                            .withColumn("error_rate", errorAggregatedDataframe
                                    .col("cnt")
                                    .multiply(1000.0)
                                    .divide(windowDuration.milliseconds()))
                            .filter((FilterFunction<Row>) row -> row.getDouble(2) > alertErrorRateTreshold)
                            .select("host", "error_rate");

            alertsDataframe.show();

            alertsDataframe
                    .selectExpr("CAST(host AS STRING) as key", "CAST(error_rate AS STRING) as value")
                    .write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBrokerServer)
                    .option("topic", alertsKafkaTopic)
                    .save();
        });

        streamingContext.start();
        streamingContext.awaitTermination();
        spark.stop();
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
