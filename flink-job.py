from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import json

def parse_json(value):
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None

# Initialize Flink environment
env = StreamExecutionEnvironment.get_execution_environment()

# Kafka Consumer
kafka_consumer = FlinkKafkaConsumer(
    topics="iot_sensor_data",  
    deserialization_schema=SimpleStringSchema(),
    properties={"bootstrap.servers": "localhost:9092", "group.id": "flink-group"}
)

data_stream = env.add_source(kafka_consumer)

parsed_stream = data_stream \
    .map(parse_json) \
    .filter(lambda x: x is not None)

# Currently just prints, should upload to cassandra or hdfs and run anomaly detection
parsed_stream.print()

env.execute("Flink Kafka Consumer Job")

