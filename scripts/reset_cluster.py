#!/usr/bin/env python3
"""
Purge and Reset Kafka Topics & Local Data
Deletes old topics from Confluent Cloud and recreates them fresh with 3 partitions.
"""

import os
import sys
import time
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
ENV_PATH = os.path.join(PROJECT_DIR, ".env")

load_dotenv(ENV_PATH)

BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("CONFLUENT_API_KEY")
API_SECRET = os.getenv("CONFLUENT_API_SECRET")

TOPICS = [
    "device_counts",
    "system_counts",
    "count_mismatches",
    "device_counts_dlq"
]

def reset_cluster():
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

    print("--- 1. Deleting existing topics from Confluent Cloud ---")
    del_futures = admin.delete_topics(TOPICS)
    for topic, future in del_futures.items():
        try:
            future.result()
            print(f"[DELETED] Topic '{topic}' deleted successfully.")
        except Exception as e:
            print(f"[INFO] Topic '{topic}': {e}")

    print("\nWaiting 5 seconds for topic deletion propagation...")
    time.sleep(5)

    print("\n--- 2. Recreating fresh topics (3 partitions each) ---")
    new_topics = [NewTopic(t, num_partitions=3, replication_factor=3) for t in TOPICS]
    create_futures = admin.create_topics(new_topics)
    for topic, future in create_futures.items():
        try:
            future.result()
            print(f"[CREATED] Topic '{topic}' created fresh with 3 partitions.")
        except Exception as e:
            print(f"[ERROR] Failed to create '{topic}': {e}")

    print("\n[OK] Kafka Cluster reset completed successfully!")

if __name__ == "__main__":
    reset_cluster()
