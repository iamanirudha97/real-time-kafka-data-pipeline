
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config.kafka_config import (
    kafka_config,
    schema_registry_config,
)


ROOT = Path(__file__).resolve().parent.parent
TOPIC = "session_start"

rng = random.Random(123)

STATION_IDS = [
    f"ST-{city}-{i:03d}"
    for city in ["BOS", "NYC", "CHI", "AUS", "SFO"]
    for i in range(1, 4)
]

PAYMENT_METHODS = [
    "CREDIT_CARD",
    "APP_WALLET",
    "RFID_CARD",
]


def main():
    # Load the new Avro schema
    schema_text = (
        ROOT / "schemas" / "session_start_v2.avsc"
    ).read_text(encoding="utf-8")

    sr_client = SchemaRegistryClient(
        schema_registry_config()
    )

    serializer = AvroSerializer(
        sr_client,
        schema_text,
    )

    producer = SerializingProducer({
        **kafka_config(),
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": serializer,
        "acks": "all",
        "enable.idempotence": True,
    })

    failures = []

    def delivery_report(err, msg):
        if err is not None:
            failures.append(str(err))
            print(f"Delivery failed: {err}")
        else:
            print(
                f"Delivered {msg.key().decode('utf-8')} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()}"
            )

    base_time = datetime.now(timezone.utc)

    # Unique IDs to avoid confusing these
    # events with the original 500 sessions.
    for i in range(501, 511):
        event = {
            "session_id": f"SESS-{i:06d}",
            "station_id": rng.choice(STATION_IDS),
            "connector_id": rng.randint(1, 2),
            "vehicle_id": f"EV-V2-{i:06d}",
            "user_id": f"USR-V2-{i:06d}",
            "start_time": int(
                (
                    base_time
                    + timedelta(seconds=(i - 500) * 14)
                ).timestamp() * 1000
            ),
            "start_soc_pct": rng.randint(5, 60),
            "payment_method": rng.choice(
                PAYMENT_METHODS
            ),
        }

        producer.produce(
            topic=TOPIC,
            key=event["session_id"],
            value=event,
            on_delivery=delivery_report,
        )

        print(
            f"Producing {event['session_id']} "
            f"payment={event['payment_method']}"
        )

        producer.poll(0)

    remaining = producer.flush(30)

    if failures or remaining:
        raise RuntimeError(
            f"Delivery failures: {len(failures)}, "
            f"messages still queued: {remaining}"
        )

    print("\nSuccessfully delivered 10 v2 session starts.")


if __name__ == "__main__":
    main()
