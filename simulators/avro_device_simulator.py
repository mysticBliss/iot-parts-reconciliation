#!/usr/bin/env python3
"""
Avro Device Telemetry Simulator with Schema Registry
Publishes Avro-encoded events to Confluent Cloud Kafka with Schema Registry registration.
Used for Phase 4: Schema Evolution & Compatibility testing.
"""

import os
import sys
import time
import argparse
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("CONFLUENT_API_KEY")
API_SECRET = os.getenv("CONFLUENT_API_SECRET")

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")

TOPIC = "device_counts"
LINE_NAME = "line1"
DEVICES = [f"pi-{i:02d}" for i in range(1, 11)]

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "part_event.avsc")

def get_iso_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def load_avro_schema():
    with open(SCHEMA_PATH, "r") as f:
        return f.read()

def run_avro_simulator(rate_sec=2.0):
    if not BOOTSTRAP_SERVERS or not SCHEMA_REGISTRY_URL:
        print("ERROR: Confluent Cloud or Schema Registry credentials missing in .env")
        sys.exit(1)

    schema_str = load_avro_schema()

    # Schema Registry Client
    sr_conf = {
        'url': SCHEMA_REGISTRY_URL,
        'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
    }
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        lambda event, ctx: event
    )
    string_serializer = StringSerializer('utf_8')

    producer_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET
    }
    producer = Producer(producer_conf)

    print(f"Connected to Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"Publishing Avro serialized events to topic '{TOPIC}'...")

    try:
        while True:
            for dev in DEVICES:
                event_dict = {
                    "device_id": dev,
                    "line": LINE_NAME,
                    "parts": 1,
                    "ts": get_iso_timestamp()
                }

                producer.produce(
                    topic=TOPIC,
                    key=string_serializer(dev),
                    value=avro_serializer(event_dict, SerializationContext(TOPIC, MessageField.VALUE))
                )
                print(f"  [AVRO PRODUCED] key={dev} -> {event_dict}")

            producer.flush()
            time.sleep(rate_sec)

    except KeyboardInterrupt:
        print("\nStopping Avro simulator...")
    finally:
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish Avro serialized events with Confluent Schema Registry")
    parser.add_argument("--rate", type=float, default=2.0, help="Interval in seconds between production ticks")
    args = parser.parse_args()

    run_avro_simulator(rate_sec=args.rate)
