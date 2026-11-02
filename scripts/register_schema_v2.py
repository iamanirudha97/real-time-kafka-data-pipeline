
from pathlib import Path

from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
)
from confluent_kafka.schema_registry.error import SchemaRegistryError

from config.kafka_config import schema_registry_config


# Project root directory
ROOT = Path(__file__).resolve().parent.parent

# Schema Registry subject
SUBJECT = "session_start-value"

# Path to the v2 Avro schema
SCHEMA_PATH = ROOT / "schemas" / "session_start_v2.avsc"


def main():

    # 1. Initialize Schema Registry client
    client = SchemaRegistryClient(
        schema_registry_config()
    )

    # 2. Read the Avro schema file
    schema_text = SCHEMA_PATH.read_text(
        encoding="utf-8"
    )

    # 3. Check compatibility configuration
    # First check subject-level compatibility.
    # If no override exists, fall back to global.
    try:
        compatibility = client.get_compatibility(
            subject_name=SUBJECT
        )

    except SchemaRegistryError as e:
        if e.error_code == 40408:
            compatibility = client.get_compatibility()
        else:
            raise

    print(f"Compatibility mode: {compatibility}")

    if compatibility != "BACKWARD":
        raise RuntimeError(
            f"Expected BACKWARD compatibility, "
            f"but found {compatibility}"
        )

    # 4. Create Avro Schema object
    schema = Schema(
        schema_str=schema_text,
        schema_type="AVRO",
    )

    # 5. Test compatibility before registration
    is_compatible = client.test_compatibility(
        subject_name=SUBJECT,
        schema=schema,
    )

    if not is_compatible:
        raise RuntimeError(
            "Schema v2 is not backward compatible "
            "with the existing schema."
        )

    print("Schema compatibility check passed!")

    # 6. Register the new schema
    schema_id = client.register_schema(
        subject_name=SUBJECT,
        schema=schema,
    )

    print(f"Registered schema ID: {schema_id}")

    # 7. Retrieve all schema versions
    versions = client.get_versions(
        subject_name=SUBJECT
    )

    print(f"Subject versions: {versions}")

    # 8. Retrieve latest schema version
    latest_schema = client.get_latest_version(
        subject_name=SUBJECT
    )

    print(
        f"Latest schema version: "
        f"{latest_schema.version}"
    )

    print(
        "\nSchema v2 registration completed successfully!"
    )


if __name__ == "__main__":
    main()