import os
import argparse
import time
from typing import Any, Optional, cast

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
)
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from config.kafka_config import (
    kafka_config,
    schema_registry_config,
)


TOPIC = "session_start"

REBALANCE_ERRORS = {
    KafkaError.ILLEGAL_GENERATION,
    KafkaError.UNKNOWN_MEMBER_ID,
    KafkaError.REBALANCE_IN_PROGRESS,
}


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--instance",
        required=True,
        help="Unique name for this consumer instance",
    )

    parser.add_argument(
        "--group",
        default="session-processing-group",
        help="Kafka consumer group ID",
    )

    parser.add_argument(
        "--simulate-crash",
        action="store_true",
        help="Crash after processing the first message, before committing",
    )

    args = parser.parse_args()

    # Initialize Schema Registry and Avro deserializer
    sr_client = SchemaRegistryClient(
        schema_registry_config()
    )

    deserializer = AvroDeserializer(sr_client)

    # Configure Kafka consumer
    consumer = Consumer({
        **kafka_config(),
        "group.id": args.group,
        "client.id": args.instance,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 45000,
        "heartbeat.interval.ms": 3000,
        "max.poll.interval.ms": 300000,
    })

    # Called when Kafka assigns partitions
    def on_assign(consumer, partitions):
        assigned = [
            (p.topic, p.partition)
            for p in partitions
        ]

        print(
            f"\n[{args.instance}] ASSIGNED: {assigned}",
            flush=True,
        )

    # Called when Kafka revokes partitions
    def on_revoke(consumer, partitions):
        revoked = [
            (p.topic, p.partition)
            for p in partitions
        ]

        print(
            f"\n[{args.instance}] REVOKED: {revoked}",
            flush=True,
        )

    # Subscribe to topic with rebalance callbacks
    consumer.subscribe(
        [TOPIC],
        on_assign=on_assign,
        on_revoke=on_revoke,
    )

    print(
        f"[{args.instance}] Started "
        f"group={args.group}, topic={TOPIC}",
        flush=True,
    )

    try:
        while True:
            # Poll Kafka for messages
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            # msg.topic() / msg.offset() are typed as Optional; narrow them.
            # Real consumed messages always have both.
            topic = msg.topic()
            offset = msg.offset()
            if topic is None or offset is None:
                continue

            # Deserialize Avro message
            # AvroDeserializer is typed to return `object`; at runtime it's a dict.
            event = cast(Optional[dict[str, Any]], deserializer(
                msg.value(),
                SerializationContext(
                    topic,
                    MessageField.VALUE,
                ),
            ))

            if event is None:
                continue

            print(
                f"[{args.instance}] "
                f"partition={msg.partition()} "
                f"offset={offset} "
                f"session={event['session_id']}",
                flush=True,
            )

            # Simulate successful message processing
            time.sleep(0.1)

            if args.simulate_crash:
                print(
                    f"[{args.instance}] SIMULATED CRASH "
                    f"after processing session={event['session_id']} "
                    f"partition={msg.partition()} "
                    f"offset={msg.offset()} "
                    "BEFORE committing",
                    flush=True,
                )
                os._exit(1)

            # Commit offset only after processing
            try:
                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

                print(
                    f"[{args.instance}] "
                    f"COMMITTED partition={msg.partition()} "
                    f"next_offset={offset + 1}",
                    flush=True,
                )

            except KafkaException as e:
                error = e.args[0]

                if error.code() in REBALANCE_ERRORS:
                    print(
                        f"[{args.instance}] "
                        f"Commit failed during rebalance: "
                        f"{error}",
                        flush=True,
                    )

                    print(
                        f"[{args.instance}] "
                        "Offset was NOT committed. "
                        "Message may be processed again.",
                        flush=True,
                    )

                    # Continue polling so the consumer
                    # can complete group rebalancing.
                    continue

                # Unexpected errors should not be hidden
                raise

    except KeyboardInterrupt:
        print(
            f"\n[{args.instance}] Shutting down...",
            flush=True,
        )

    finally:
        consumer.close()

        print(
            f"[{args.instance}] Consumer closed.",
            flush=True,
        )


if __name__ == "__main__":
    main()
