# [WIP] SmartCity-Spark-Streaming Pipeline

## Project Setup
1. In `jobs` directory, create app.config with the following template
```
[AWS]
ACCESS_KEY='<YOUR_KEY>'
ACCESS_SECRET='<YOUR_SECRET>'
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
