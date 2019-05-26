package com.alex.home.spark;

import com.alex.home.*;
import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.*;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class StreamProcessorTests {
    private static JavaSparkContext sparkContext;
    private JavaStreamingContext streamingContext;

    @BeforeClass
    public static void setup() {
        SparkConf conf = new SparkConf()
                .setAppName("test")
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts", "true");;
        sparkContext = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void clean() throws Exception {
        if (sparkContext != null) {
            sparkContext.stop();
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
    }

    @Before
    public void createStreamingContext() throws Exception {
        streamingContext = new JavaStreamingContext(sparkContext, Seconds.apply(1));
    }

    @After
    public void tearDownStreamingContext() throws Exception {
        if (streamingContext != null && streamingContext.getState() != StreamingContextState.STOPPED) {
            streamingContext.stop(false, true);
        }
    }

    @Test
    public void aggregateErrorLogs_shouldReturnErrorAlerts()
    {
        final double threshold = 0.3;
        JavaDStream<LogMessageItem> streamLogs = getErrorStreamLogs(streamingContext);

        JavaDStream<ErrorAlert> stream = StreamProcessor.aggregateErrorLogs(
                streamLogs, 3000, threshold);

        List<ErrorAlert> result = streamToList(stream);

        streamingContext.start();
        try {
            streamingContext.awaitTerminationOrTimeout(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.stream().filter(n -> "host1".equals(n.getHost()) && n.getError_rate() > threshold).count());
        Assert.assertEquals(2, result.stream().filter(n -> "host2".equals(n.getHost()) && n.getError_rate() > threshold).count());
    }

    @Test
    public void aggregateLogs_shouldReturnAggregatedValues()
    {
        JavaDStream<LogMessageItem> streamLogs = getStreamLogs(streamingContext);

        JavaPairDStream<LogInfoKey, LogMeasurement> stream =
                StreamProcessor.aggregateLogs(streamLogs, 3000);

        List<Tuple2<LogInfoKey, LogMeasurement>> result = streamToList(stream);

        streamingContext.start();
        try {
            streamingContext.awaitTerminationOrTimeout(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertNotNull(result);
        Assert.assertEquals(10, result.stream()
                .filter(n -> LogLevel.TRACE.equals(n._1.getLevel()))
                .mapToLong(n -> n._2.getCount())
                .sum()
        );
        Assert.assertEquals(5, result.stream()
                .filter(n -> LogLevel.WARN.equals(n._1.getLevel()))
                .mapToLong(n -> n._2.getCount())
                .sum()
        );
        Assert.assertEquals(2, result.stream()
                .filter(n -> LogLevel.ERROR.equals(n._1.getLevel()))
                .mapToLong(n -> n._2.getCount())
                .sum()
        );
    }

    private static JavaDStream<LogMessageItem> getErrorStreamLogs(JavaStreamingContext context) {
        Queue<JavaRDD<LogMessageItem>> queue = new LinkedList<>();

        List<LogMessageItem> list1 = Arrays.asList(
                new LogMessageItem("host1", LogLevel.ERROR, ""),
                new LogMessageItem("host2", LogLevel.ERROR, ""),
                new LogMessageItem("host1", LogLevel.ERROR, ""),
                new LogMessageItem("host3", LogLevel.WARN, "")
        );
        List<LogMessageItem> list2 = Arrays.asList(
                new LogMessageItem("host2", LogLevel.ERROR, ""),
                new LogMessageItem("host3", LogLevel.TRACE, ""),
                new LogMessageItem("host3", LogLevel.INFO, ""),
                new LogMessageItem("host3", LogLevel.WARN, "")
        );

        queue.add(sparkContext.parallelize(list1));
        queue.add(sparkContext.parallelize(list2));

        return context.queueStream(queue, true);
    }

    private static JavaDStream<LogMessageItem> getStreamLogs(JavaStreamingContext context) {
        Queue<JavaRDD<LogMessageItem>> queue = new LinkedList<>();

        Pair<String, LogMessage> traces = MessageGenerator.generate(10, LogLevel.TRACE);
        Pair<String, LogMessage> warnings = MessageGenerator.generate(5, LogLevel.WARN);
        Pair<String, LogMessage> errors = MessageGenerator.generate(2, LogLevel.ERROR);

        queue.add(sparkContext.parallelize(traces.getValue().getItems()));
        queue.add(sparkContext.parallelize(warnings.getValue().getItems()));
        queue.add(sparkContext.parallelize(errors.getValue().getItems()));

        return context.queueStream(queue, true);
    }

    private static <T> List<T> streamToList(JavaDStream<T> stream) {
        List<T> list = new ArrayList<>();
        stream.foreachRDD(rdd -> {
            List<T> tmp = rdd.collect();
            list.addAll(tmp);
        });
        return list;
    }

    private static <T, V> List<Tuple2<T, V>> streamToList(JavaPairDStream<T, V> stream) {
        List<Tuple2<T, V>> list = new ArrayList<>();
        stream.foreachRDD(rdd -> {
            List<Tuple2<T, V>> tmp = rdd.collect();
            list.addAll(tmp);
        });
        return list;
    }
}
