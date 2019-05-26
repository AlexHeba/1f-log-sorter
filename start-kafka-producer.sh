
export KAFKA_BROKER_SERVER="localhost:9092"
export INPUT_KAFKA_TOPIC="input"

java -cp ./kafka_app/target/kafka_app-1.0.jar com.alex.home.kafka.SimpleKafkaProducer "$INPUT_KAFKA_TOPIC" "$KAFKA_BROKER_SERVER" "$1" "$2"

