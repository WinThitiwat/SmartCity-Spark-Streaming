import os
import uuid
import random
import time

from confluent_kafka import SerializingProducer
from datetime import datetime, timedelta
from util.kafka_service import KafkaUtils
from publishing.smartcity import SmartCityDataGenerator
import config as cfg

def simulate_journey(kafka_producer, device_id):
    datagen = SmartCityDataGenerator(datetime.now())
    kafka_utils = KafkaUtils()
    
    while True:
        
        vehicle_data = datagen.generate_vehicle_data(device_id)
        gps_data = datagen.generate_gps_data(device_id, vehicle_data["timestamp"])
        traffic_camera_data = datagen.generate_traffic_camera_data(device_id, vehicle_data["timestamp"], vehicle_data["location"], "Security-Camera-1")
        weather_data = datagen.generate_weather_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])
        emergency_incident_data = datagen.generate_emergency_incident_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])
        
        # print(f"vehicle_data: {vehicle_data}")
        if (vehicle_data["location"][0] >= datagen.BIRMINGHAM_COORDINATES["latitude"]
            and vehicle_data["location"][1] <= datagen.BIRMINGHAM_COORDINATES["longitude"]):
            print("Vehicle has reached Birmingham. Simulation ending...")
            break
        
        kafka_utils.write_to_kafka(kafka_producer, cfg.VEHICLE_TOPIC, str(vehicle_data["id"]), vehicle_data)
        kafka_utils.write_to_kafka(kafka_producer, cfg.GPS_TOPIC, str(gps_data["id"]), gps_data)
        kafka_utils.write_to_kafka(kafka_producer, cfg.TRAFFIC_TOPIC, str(traffic_camera_data["id"]), traffic_camera_data)
        kafka_utils.write_to_kafka(kafka_producer, cfg.WEATHER_TOPIC, str(weather_data["id"]), weather_data)
        kafka_utils.write_to_kafka(kafka_producer, cfg.EMERGENCY_TOPIC, str(emergency_incident_data["id"]), emergency_incident_data)

        time.sleep(3)

        # break


if __name__ == "__main__":
    producer_config = {
        "bootstrap.servers": cfg.KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print(f"Kafka error: {err}"), # error callback 
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, "Vehicle-test")
    except KeyboardInterrupt:
        print(f"\nSimulation ended by the user")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")