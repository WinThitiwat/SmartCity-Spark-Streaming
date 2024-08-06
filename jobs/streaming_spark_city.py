import config as cfg
from pyspark.sql import SparkSession
from models.streaming_model import SmartCity
from util.kafka_service import KafkaUtils
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def generate_spark_session(app_name, configs) -> "SparkSession":
    """
    A function to generate Spark session based on the passed-in configuration.

    Example
    
        >>> spark_config = {
        ...    "hive.exec.dynamic.partition" : True,
        ...    "hive.exec.dynamic.partition.mode": "nonstrict",
        ...    "spark.sql.parquet.compression.codec": "gzip",
        ...    "enableHiveSupport": True
        ... }

        >>> gen_spark_session("my_spark_test", spark_config)  
    """
    spark_builder = SparkSession.builder.appName(app_name)
    for config_name, config_val in configs.items():
        if config_name != "app_name":
            spark_builder = spark_builder.config(config_name, config_val)
    if configs.get("enableHiveSupport", False):
        spark_builder = spark_builder.enableHiveSupport()
    return spark_builder.getOrCreate()

def streamWriter(inputDF: DataFrame, checkpointFolder: "str", outputLocation: "str"):
    return (
        inputDF
        .writeStream
        .format("parquet")
        .option("checkpointLocation", checkpointFolder)
        .option("path", outputLocation)
        .outputMode("append")
        .start()
    )
      

def main():
    
    configs = {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            + "org.apache.hadoop:hadoop-aws:3.3.1,"
            # + "org.apache.kafka:kafka-clients:3.6.0,"
            + "com.amazonaws:aws-java-sdk:1.11.469",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": cfg.AWS_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": cfg.AWS_SECRET_KEY,
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }

    print("Init spark session...")
    spark = generate_spark_session("SmartCityStreamingSpark", configs)

    spark.sparkContext.setLogLevel("WARN")

    vehicleDF = KafkaUtils().read_kafka_topic(spark, cfg.KAFKA_BOOTSTRAP_SERVERS_V2, cfg.VEHICLE_TOPIC, SmartCity.VEHICLE_SCHEMA).alias("vehicle")
    gpsDF = KafkaUtils.read_kafka_topic(spark, cfg.KAFKA_BOOTSTRAP_SERVERS_V2, cfg.GPS_TOPIC, SmartCity.GPS_SCHEMA).alias("gps")
    trafficDF = KafkaUtils.read_kafka_topic(spark, cfg.KAFKA_BOOTSTRAP_SERVERS_V2, cfg.TRAFFIC_TOPIC, SmartCity.TRAFFIC_SCHEMA).alias("traffic")
    weatherDF = KafkaUtils.read_kafka_topic(spark, cfg.KAFKA_BOOTSTRAP_SERVERS_V2, cfg.WEATHER_TOPIC, SmartCity.WEATHER_SCHEMA).alias("weather")
    emergencyDF = KafkaUtils.read_kafka_topic(spark, cfg.KAFKA_BOOTSTRAP_SERVERS_V2, cfg.EMERGENCY_TOPIC, SmartCity.EMERGENCY_SCHEMA).alias("emergency")


    query1 = streamWriter(vehicleDF, cfg.S3_CHECKPOINTS["vehicle_data"], cfg.S3_DATA["vehicle_data"])
    query2 = streamWriter(gpsDF, cfg.S3_CHECKPOINTS["gps_data"], cfg.S3_DATA["gps_data"])
    query3 = streamWriter(trafficDF, cfg.S3_CHECKPOINTS["traffic_data"], cfg.S3_DATA["traffic_data"])
    query4 = streamWriter(weatherDF, cfg.S3_CHECKPOINTS["weather_data"], cfg.S3_DATA["weather_data"])
    query5 = streamWriter(emergencyDF, cfg.S3_CHECKPOINTS["emergency_data"], cfg.S3_DATA["emergency_data"])

    # query1.awaitTermination()
    query5.awaitTermination()

if __name__ == "__main__":
    main()