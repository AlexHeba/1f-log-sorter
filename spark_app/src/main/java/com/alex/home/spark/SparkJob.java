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
        String sparkMasterUrl = "spark://alex-vb:7077";
        String kafkaBrokerServer = "localhost:9092";
        Duration slideDuration = Durations.seconds(10);
        Duration windowDuration = Durations.seconds(60);
        String inputKafkaTopic = "input2";
        String outputKafkaTopic = "output";
        String alertsKafkaTopic = "alerts3";
        Double alertErrorRateTreshold = 1.0;

//        sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
//        if(sparkMasterUrl == null) {
//            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
//        }

        SparkConf sparkConf = new SparkConf().setAppName("alex-spark").setMaster(sparkMasterUrl);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, slideDuration);

        Map<String, Object> kafkaParams = KafkaClientUtils.getConsumerProperties(
                kafkaBrokerServer,
                StringDeserializer.class,
                JsonDeserializer.class);
//        kafkaParams.put("bootstrap.servers", "localhost:9092");
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", JsonDeserializer.class);
//        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
//        kafkaParams.put("auto.offset.reset", "latest");
//        kafkaParams.put("enable.auto.commit", false);
//        kafkaParams.put(JsonDeserializer.TRUSTED_PACKAGES, "com.alex.home");

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
}
