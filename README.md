# [WIP] SmartCity-Spark-Streaming Pipeline
This is to simulate a situation of a smart city where streaming data is being generated from various sources, such as vehicle, gps, traffic camera, weather monitoring system, and emergency info. This real-time pipeline is here to support the data accessibility.

This project is to practice real-time data pipeline using Apache Kafka to stream data, Spark Streaming to process the data in real-time manner, and sink the data into S3 bucket as data lake. Later on, the S3 datalake is being analyzed by AWS Glue Catalog to manage the metadata, and then being used by analytical purpose, such as Athena, or Quicksight.


## System Architecture
WIP

## Prerequisite Setup
### Project Setup
1. In `jobs` directory, create `app.config` with the following template
```
[AWS]
ACCESS_KEY='<YOUR_KEY>'
ACCESS_SECRET='<YOUR_SECRET>'

[S3]
BASE_CHECKPOINT=<S3-Bucket-Name-URI>/checkpoints
BASE_DATA=<S3-Bucket-Name-URI>/data
```
2. Make sure to create virtual environment to test on local (if not on Docker)

### Amazon S3 Bucket setup
1. Make sure to set up your Amazon S3 and bucket to store the data. Below is the bucket and directory structure required by the codebase.

   ```
   Bucket-Name
     - checkpoints
     - data
   ```


## To run
0. Spin up the docker images. Check if all are in running state.
```
SmartCity-Spark-Streaming > docker compose up -d
```
1. To publish data to Kafka topic, in `jobs` directory, run the `main.py` file
```
SmartCity-Spark-Streaming/jobs > python main.py
```
To verify the topics are being created in Kafka, go to Docker exec terminal and run
```
kafka-topics --list --bootstrap-server localhost:9092
```

To verify if the data is being published to the topic, go to Docker exec terminal and run

```
kafka-console-consumer --topic gps_data --bootstrap-server localhost:9092 --from-beginning
```

2. To extract data from Kafka topic and load to S3, at the root project directory, run the spark-submit bash file
```
SmartCity-Spark-Streaming > ./spark-submit.sh
```
