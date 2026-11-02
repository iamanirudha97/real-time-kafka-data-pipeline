
import argparse
from pathlib import Path
from typing import Any, Optional, cast

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from config.kafka_config import (
    kafka_config,
    schema_registry_config,
)

ROOT = Path(__file__).resolve().parent.parent
TOPIC = "session_start"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version",
        choices=["v1", "v2"],
        default="v1",
    )

    parser.add_argument(
        "--group",
        required=True,
    )

    args = parser.parse_args()

    schema_file = (
        ROOT
        / "schemas"
        / f"session_start_{args.version}.avsc"
    )

    reader_schema = schema_file.read_text()

    sr_client = SchemaRegistryClient(
        schema_registry_config()
    )

    deserializer = AvroDeserializer(
        sr_client,
        schema_str=reader_schema,
    )

    consumer = DeserializingConsumer({
        **kafka_config(),
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": deserializer,
    })

    consumer.subscribe([TOPIC])

    print(
        f"Running {args.version} consumer "
        f"in group {args.group}"
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # The type stubs declare msg.value() as bytes, but at runtime the
            # AvroDeserializer has already turned it into a dict.
            event = cast(Optional[dict[str, Any]], msg.value())

            if event is None:
                continue

            print(
                f"Session: {event['session_id']} | "
                f"Station: {event['station_id']} | "
                f"Payment: {event.get('payment_method', 'NOT_IN_V1')}"
            )

    except KeyboardInterrupt:
        print("\nStopping consumer")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
