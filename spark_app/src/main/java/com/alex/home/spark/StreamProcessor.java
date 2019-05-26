package com.alex.home.spark;

import com.alex.home.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

public class StreamProcessor {
    public static JavaPairDStream<LogInfoKey, LogMeasurement> aggregateLogs(JavaDStream<LogMessageItem> stream, long windowDuration) {
        return stream
                .mapToPair(n -> new Tuple2<>(new LogInfoKey(n.getHost(), n.getLevel()), 1))
                .reduceByKey((agg, cur) -> agg + cur)
                .mapToPair(n -> new Tuple2<>(n._1, new LogMeasurement(n._2, n._2 * 1000.0 / windowDuration)));
    }

    public static JavaDStream<ErrorAlert> aggregateErrorLogs(
            JavaDStream<LogMessageItem> stream, long windowDuration, double alerThreshold) {
        return stream
                .filter(n -> LogLevel.ERROR.equals(n.getLevel()))
                .mapToPair(n -> new Tuple2<>(n.getHost(), 1))
                .reduceByKey((agg, cur) -> agg + cur)
                .map(n -> new ErrorAlert(n._1, n._2 * 1000.0 / windowDuration))
                .filter(n -> n.getError_rate() > alerThreshold);
    }
}
