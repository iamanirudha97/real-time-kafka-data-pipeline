
import os
from dotenv import load_dotenv

load_dotenv()

def kafka_config():
    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
    }

def schema_registry_config():
    return {
        "url": os.environ["SCHEMA_REGISTRY_URL"],
        "basic.auth.user.info": (
            f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:"
            f"{os.environ['SCHEMA_REGISTRY_API_SECRET']}"
        ),
    }
