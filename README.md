# 1f-log-sorter

## Build
1. build with maven: "mvn clean install";
2. start your Kafka server;
3. start your Spark master and workers;
4. set your configures on 'spark-submit.sh': 
    - SPARK_WORK_FOLDER - path to your Spark folder,
    - SPARK_MASTER_URL - URL of Spark master node,
    - KAFKA_BROKER_SERVER - URL of Kafka broker node;
5. (optional) set your others configures
    - INPUT_KAFKA_TOPIC="input0",
    - OUT_KAFKA_TOPIC="output0",
    - ALERT_KAFKA_TOPIC="alerts0",
    - WINDOW_DURATION_SEC="60",
    - SLIDE_DURATION_SEC="10",
    - ALERT_ERROR_RATE_THERSHOLD="1.0"
6. run 'spark-submit.sh'
7. (optional) set your configures on 'start-kafka-producer.sh': KAFKA_BROKER_SERVER and INPUT_KAFKA_TOPIC, and run it; you can also pass parameters: first is number - degree of producers parallel, second is string - log level, messages with this level only will be produced
