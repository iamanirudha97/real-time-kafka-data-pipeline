
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config.kafka_config import (
    kafka_config,
    schema_registry_config,
)

fake = Faker("en_US")
rng = random.Random(42)
Faker.seed(42)

ROOT = Path(__file__).resolve().parent.parent

CITIES = {
    "BOS": "Boston",
    "NYC": "New York",
    "CHI": "Chicago",
    "AUS": "Austin",
    "SFO": "San Francisco",
}

# Matches the original 15 stations.
# For now, connectors are limited to 1 or 2,
# which is valid for every station we generated.
STATION_IDS = [
    f"ST-{code}-{i:03d}"
    for code in CITIES
    for i in range(1, 4)
]


def load_schema(filename):
    return (ROOT / "schemas" / filename).read_text()


def build_producer(schema_filename):
    sr_client = SchemaRegistryClient(
        schema_registry_config()
    )

    serializer = AvroSerializer(
        sr_client,
        load_schema(schema_filename),
    )

    return SerializingProducer({
        **kafka_config(),
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": serializer,
        "acks": "all",
        "enable.idempotence": True,
    })


def delivery_report(err, msg):
    key = msg.key().decode("utf-8") if msg.key() else None

    if err is not None:
        print(
            f"Delivery failed for key={key} "
            f"topic={msg.topic()}: {err}"
        )
    else:
        print(
            f"Delivered key={key} "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def generate_sessions(count=500):
    base_time = datetime.now(timezone.utc)

    for i in range(1, count + 1):
        session_id = f"SESS-{i:06d}"
        station_id = rng.choice(STATION_IDS)

        start_soc = rng.randint(5, 60)
        end_soc = rng.randint(start_soc + 10, 95)

        duration_minutes = rng.randint(10, 90)

        # Distribute simulated starts over 2 hours.
        start_dt = base_time + timedelta(
            seconds=i * 14
        )
        stop_dt = start_dt + timedelta(
            minutes=duration_minutes
        )

        start_ms = int(start_dt.timestamp() * 1000)
        stop_ms = int(stop_dt.timestamp() * 1000)

        # Simplified 75 kWh vehicle battery model.
        energy_kwh = round(
            75 * (end_soc - start_soc) / 100, 2
        )

        start_event = {
            "session_id": session_id,
            "station_id": station_id,
            "connector_id": rng.randint(1, 2),
            "vehicle_id": f"EV-{fake.unique.random_number(digits=6)}",
            "user_id": f"USR-{fake.random_number(digits=5)}",
            "start_time": start_ms,
            "start_soc_pct": start_soc,
        }

        # 7% intentionally have no stop.
        stop_event = None

        if rng.random() >= 0.07:
            stop_event = {
                "session_id": session_id,
                "station_id": station_id,
                "stop_time": stop_ms,
                "end_soc_pct": end_soc,
                "energy_kwh": energy_kwh,
                "stop_reason": rng.choices(
                    ["COMPLETED", "USER_STOPPED", "FAULT"],
                    weights=[90, 7, 3],
                )[0],
            }

        yield start_event, stop_event


def main():
    start_producer = build_producer(
        "session_start_v1.avsc"
    )
    stop_producer = build_producer(
        "session_stop.avsc"
    )

    starts = 0
    stops = 0
    failures = []

    def report(err, msg):
        if err is not None:
            failures.append(str(err))

    for start_event, stop_event in generate_sessions():
        session_id = start_event["session_id"]

        start_producer.produce(
            topic="session_start",
            key=session_id,
            value=start_event,
            on_delivery=report,
        )
        starts += 1

        if stop_event is not None:
            stop_producer.produce(
                topic="session_stop",
                key=session_id,
                value=stop_event,
                on_delivery=report,
            )
            stops += 1

        start_producer.poll(0)
        stop_producer.poll(0)

    remaining_start = start_producer.flush(30)
    remaining_stop = stop_producer.flush(30)

    if failures or remaining_start or remaining_stop:
        raise RuntimeError(
            f"Delivery errors: {len(failures)}, "
            f"queued starts: {remaining_start}, "
            f"queued stops: {remaining_stop}"
        )

    print(f"Session starts delivered: {starts}")
    print(f"Session stops delivered: {stops}")
    print(f"Unmatched starts: {starts - stops}")


if __name__ == "__main__":
    main()
