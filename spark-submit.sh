SPARK_WORK_FOLDER="/home/alex/Applications/spark-2.4.3-bin-hadoop2.7"
MY_SPARK_JAR="./spark_app/target/spark_app-1.0.jar"
MY_SPARK_CLASS="com.alex.home.spark.SparkJob"

export SPARK_MASTER_URL="spark://localhost:7078"
export KAFKA_BROKER_SERVER="localhost:9092"
export INPUT_KAFKA_TOPIC="input0"
export OUT_KAFKA_TOPIC="output0"
export ALERT_KAFKA_TOPIC="alerts0"
export WINDOW_DURATION_SEC="60"
export SLIDE_DURATION_SEC="10"
export ALERT_ERROR_RATE_THERSHOLD="1.0"


"$SPARK_WORK_FOLDER"/bin/spark-submit --class "$MY_SPARK_CLASS" --master "$SPARK_MASTER_URL" "$MY_SPARK_JAR"
