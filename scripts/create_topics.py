#!/usr/bin/env python3
"""
Create Topics in Confluent Cloud Kafka Cluster
Creates:
 - device_counts (3 partitions)
 - system_counts (3 partitions)
 - count_mismatches (3 partitions)
 - device_counts_dlq (3 partitions)
"""

import os
import sys
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
ENV_PATH = os.path.join(PROJECT_DIR, ".env")

load_dotenv(ENV_PATH)

BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("CONFLUENT_API_KEY")
API_SECRET = os.getenv("CONFLUENT_API_SECRET")

def create_topics():
    if not BOOTSTRAP_SERVERS or not API_KEY or not API_SECRET:
        print("[ERROR] Confluent Cloud credentials missing in .env")
        sys.exit(1)

    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET
    }

    admin = AdminClient(conf)

    topics_to_create = [
        NewTopic("device_counts", num_partitions=3, replication_factor=3),
        NewTopic("system_counts", num_partitions=3, replication_factor=3),
        NewTopic("count_mismatches", num_partitions=3, replication_factor=3),
        NewTopic("device_counts_dlq", num_partitions=3, replication_factor=3),
    ]

    print(f"Connecting to Confluent Cloud ({BOOTSTRAP_SERVERS})...")
    futures = admin.create_topics(topics_to_create)

    for topic, future in futures.items():
        try:
            future.result()  # The result itself is None on success
            print(f"[OK] Topic '{topic}' created successfully.")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"[EXISTS] Topic '{topic}' already exists.")
            else:
                print(f"[ERROR] Failed to create topic '{topic}': {e}")

if __name__ == "__main__":
    create_topics()
