
import random
from faker import Faker
import random
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config.kafka_config import (
    kafka_config,
    schema_registry_config,
)

TOPIC = "charging_stations"

fake = Faker("en_US")
Faker.seed(42)

CITIES = {
    "Boston": {
        "code": "BOS",
        "lat": 42.3601,
        "lon": -71.0589,
    },
    "New York": {
        "code": "NYC",
        "lat": 40.7128,
        "lon": -74.0060,
    },
    "Chicago": {
        "code": "CHI",
        "lat": 41.8781,
        "lon": -87.6298,
    },
    "Austin": {
        "code": "AUS",
        "lat": 30.2672,
        "lon": -97.7431,
    },
    "San Francisco": {
        "code": "SFO",
        "lat": 37.7749,
        "lon": -122.4194,
    },
}

STATION_TYPES = [
    "Downtown Charging Hub",
    "Shopping Center Chargers",
    "Public Parking Garage",
    "Highway Fast Charging",
    "Community Charging Station",
]


def generate_stations(stations_per_city=3):
    rng = random.Random(42)
    stations = []

    for city, config in CITIES.items():
        for index in range(1, stations_per_city + 1):

            station_id = (
                f"ST-{config['code']}-{index:03d}"
            )

            station_name = (
                f"{fake.street_name()} "
                f"{rng.choice(STATION_TYPES)}"
            )

            station = {
                "station_id": station_id,
                "station_name": station_name,
                "city": city,
                "operator": "VoltRoute",
                "latitude": round(
                    config["lat"] +
                    rng.uniform(-0.04, 0.04), 6
                ),
                "longitude": round(
                    config["lon"] +
                    rng.uniform(-0.04, 0.04), 6
                ),
                "max_power_kw": float(
                    rng.choice([50, 150, 350])
                ),
                "num_connectors": rng.choice([2, 4, 6]),
                "tariff_per_kwh": round(
                    rng.uniform(0.25, 0.60), 2
                ),
                "status": (
                    "MAINTENANCE"
                    if index == 3 and city in
                    ["Boston", "Chicago"]
                    else "ACTIVE"
                ),
            }

            stations.append(station)

    return stations


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered {msg.key()} "
            f"to partition {msg.partition()} "
            f"at offset {msg.offset()}"
        )


def main():
    schema_path = (
        Path(__file__).resolve().parent.parent
        / "schemas"
        / "charging_stations.avsc"
    )

    schema_str = schema_path.read_text()

    sr_client = SchemaRegistryClient(
        schema_registry_config()
    )

    avro_serializer = AvroSerializer(
        sr_client,
        schema_str,
    )

    producer = SerializingProducer({
        **kafka_config(),
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "acks": "all",
    })

    stations = generate_stations()

    for station in stations:
        producer.produce(
            topic=TOPIC,
            key=station["station_id"],
            value=station,
            on_delivery=delivery_report,
        )
        producer.poll(0)

    remaining = producer.flush(30)

    if remaining:
        raise RuntimeError(
            f"{remaining} messages remain undelivered"
        )

    print(f"Finished sending {len(stations)} stations")


if __name__ == "__main__":
    main()