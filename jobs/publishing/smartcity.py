import uuid
import random
from datetime import datetime, timedelta


class _SmartCityDataGenenatorHelper():
    LONDON_COORDINATES = { "latitude": 51.5074, "longitude": -0.1278 }
    BIRMINGHAM_COORDINATES = { "latitude": 52.4862, "longitude": -1.8904 }

    # Calculate movement increments
    LATITUDE_INCREMENTAL = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]) / 100
    LONGITUDE_INCREMENTAL = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]) / 100

    def __init__(self, start_time=datetime.now(), random_seed=45) -> None:
        random.seed(random_seed)
        self.start_time = start_time
        self.start_location = self.LONDON_COORDINATES.copy()

    def _get_next_time(self):
        self.start_time += timedelta(seconds=random.randint(30, 60)) # update frequency
        return self.start_time
    

    def _simulate_vehicle_movement(self):
        # move towards some city
        self.start_location["latitude"] += self.LATITUDE_INCREMENTAL
        self.start_location["longitude"] += self.LONGITUDE_INCREMENTAL

        # add some randomness to simulate actual road travel
        self.start_location["latitude"] += random.uniform(-0.0005, 0.0005)
        self.start_location["longitude"] += random.uniform(-0.0005, 0.0005)

        return self.start_location

class SmartCityDataGenerator(_SmartCityDataGenenatorHelper):
    """
    Data Generator class to generate necessary components of smart city data
    """
    def __init__(self, start_time) -> None:
        super().__init__(start_time)

    def generate_emergency_incident_data(self, device_id, timestamp, location) -> "dict":
        """
        Generate emergency incident data in the format of dict
        """
        return {
            "id": uuid.uuid4(),
            "deviceId": device_id,
            "incidentId": uuid.uuid4(),
            "type": random.choice(["Accident", "Fire", "Medical", "Police", "None"]),
            "timestamp": timestamp,
            "location": location,
            "status": random.choice(["Active", "Resolved"]),
            "Description": "Description of the accident"
        }

    def generate_weather_data(self, device_id, timestamp, location):
        return {
            "id": uuid.uuid4(),
            "deviceId": device_id,
            "location": location,
            "timestamp": timestamp,
            "temperature": random.uniform(-5, 30),  # celcius
            "weatherCondition": random.choice(["Sunny", "Cloudy", "Rainy", "Snowy"]),
            "precipitation": random.uniform(0, 25),
            "windSpeed": random.uniform(0, 100),
            "humidity": random.randint(0, 100),  # percentage
            "airQualityIndex": random.uniform(0, 500),
        }

    def generate_traffic_camera_data(self, device_id, timestamp, location, camera_id):
        return {
            "id": uuid.uuid4(),
            "deviceId": device_id,
            "cameraId": camera_id,
            "location": location,
            "timestamp": timestamp,
            "snapshot": "Base64EncodedString"  # could be from some object storage like S3
        }

    def generate_vehicle_data(self, device_id):
        location = self._simulate_vehicle_movement()
        return {
            "id": uuid.uuid4(),
            "deviceId": device_id,
            "timestamp": self._get_next_time().isoformat(),
            "location": (location["latitude"], location["longitude"]),
            "speed": random.uniform(10, 40),
            "direction": "North-East",
            "make": "Audi",
            "model": "RS7",
            "year": 2024,
            "fuelType": "Hybrid"
        }

    def generate_gps_data(self, device_id, timestamp, vehicle_type="private"):
        return {
            "id": uuid.uuid4(),
            "deviceId": device_id,
            "timestamp": timestamp,
            "speed": random.uniform(0, 40),  # km/hr
            "direction": "North-East",
            "vehicleType": vehicle_type
        }

