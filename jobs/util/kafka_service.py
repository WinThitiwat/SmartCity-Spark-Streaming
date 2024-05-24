
import uuid
import simplejson as json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.dataframe import DataFrame
from confluent_kafka import SerializingProducer
from pyspark.sql.types import StructType

class KafkaUtils:

    def _json_serializer(self, obj) -> "str":
        if isinstance(obj, uuid.UUID):
            return str(obj)
        raise TypeError(f"Error - Object of type {obj.__class__.__name__} is not JSON serializable!!")

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    @staticmethod
    def read_kafka_topic(
                        spark: "SparkSession",
                        servers: "str",
                        src_topic: "str",
                        schema: "StructType",
                        offset = "earliest") -> "DataFrame":
        """
        Read streaming data from source
        """
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", servers) \
            .option("subscribe", src_topic) \
            .option("startingOffsets", offset)
        
        return (
            df
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
            .withWatermark("timestamp", "2 minutes")
        )

    def write_to_kafka(self,
                       kafka_producer: "SerializingProducer",
                       sink_topic: "str",
                       sink_key: "str",
                       sink_payload: "dict"
                       ) -> "None":
        """
        Kafka producer to write data
        """

        kafka_producer.produce(
            topic = sink_topic,
            key = sink_key,
            value = json.dumps(sink_payload, default = self._json_serializer).encode("utf-8"),
            on_delivery = self._delivery_report
        )
        kafka_producer.flush()