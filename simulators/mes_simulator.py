#!/usr/bin/env python3
"""
MES (Manufacturing Execution System) Simulator
System of Record publisher producing counts directly to Confluent Cloud 'system_counts'.
Supports multi-line factory topology and both delimited (<device_id>::<line>::<parts>::<status>::<ts>) and JSON.
"""

import os
import sys
import json
import time
import random
import argparse
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY = os.getenv("CONFLUENT_API_KEY")
API_SECRET = os.getenv("CONFLUENT_API_SECRET")
TOPIC = "system_counts"

MACHINE_TOPOLOGY = {
    "pi-01": "line1",
    "pi-02": "line1",
    "pi-03": "line1",
    "pi-04": "line1",
    "pi-05": "line2",
    "pi-06": "line2",
    "pi-07": "line2",
    "pi-08": "line3",
    "pi-09": "line3",
    "pi-10": "line3"
}

def get_iso_timestamp(offset_seconds=0):
    now = datetime.now(timezone.utc)
    if offset_seconds != 0:
        now = datetime.fromtimestamp(now.timestamp() + offset_seconds, timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed for record {msg.key()}: {err}", flush=True)

def format_payload(dev, line, parts, status, ts, fmt="delimited"):
    if fmt == "delimited":
        return f"{dev}::{line}::{parts}::{status}::{ts}"
    else:
        return json.dumps({
            "device_id": dev,
            "line": line,
            "parts": parts,
            "status": status,
            "ts": ts
        })

def run_mes_simulator(rate_sec=2.0, anomaly_rate=0.15, fmt="delimited"):
    if not BOOTSTRAP_SERVERS or not API_KEY or not API_SECRET:
        print("[ERROR] Confluent Cloud credentials missing in .env", flush=True)
        sys.exit(1)

    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET,
        'client.id': 'mes-simulator-client'
    }

    producer = Producer(conf)
    print(f"Connected to Confluent Cloud at {BOOTSTRAP_SERVERS}", flush=True)
    print(f"Publishing MES records to '{TOPIC}' across 3 lines (Format: {fmt.upper()}, Anomaly Rate: {anomaly_rate * 100}%)...", flush=True)

    try:
        while True:
            for dev, line in MACHINE_TOPOLOGY.items():
                rand_val = random.random()

                # Anomaly 1: Dropped Count (Missed sensor pulse)
                if rand_val < (anomaly_rate * 0.4):
                    print(f"  [MES ANOMALY] Dropped count for {dev} ({line})", flush=True)
                    continue

                # Anomaly 2: Timing Drift (Delayed entry)
                timing_drift_sec = 0
                if (anomaly_rate * 0.4) <= rand_val < (anomaly_rate * 0.7):
                    timing_drift_sec = random.randint(-45, -15)
                    print(f"  [MES ANOMALY] Timestamp drift for {dev} ({timing_drift_sec}s delay)", flush=True)

                # Anomaly 3: Multi-count Batch Miscount
                parts_count = 1
                if (anomaly_rate * 0.7) <= rand_val < anomaly_rate:
                    parts_count = random.choice([2, 3])
                    print(f"  [MES ANOMALY] Multi-count registered for {dev}: {parts_count} parts", flush=True)

                ts = get_iso_timestamp(offset_seconds=timing_drift_sec)
                status = "RUNNING"

                payload_str = format_payload(dev, line, parts_count, status, ts, fmt=fmt)

                producer.produce(
                    topic=TOPIC,
                    key=dev.encode('utf-8'),
                    value=payload_str.encode('utf-8'),
                    on_delivery=delivery_report
                )
                print(f"  [PRODUCED MES] key={dev} -> {payload_str}", flush=True)

            producer.flush()
            time.sleep(rate_sec)

    except KeyboardInterrupt:
        print("\nStopping MES simulator...", flush=True)
    finally:
        producer.flush()
        print("Flushed and stopped.", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate MES System of Record counts to Confluent Cloud")
    parser.add_argument("--rate", type=float, default=2.0, help="Interval in seconds between production ticks")
    parser.add_argument("--format", type=str, choices=["delimited", "json"], default="delimited", help="Payload format (delimited '::' or json)")
    parser.add_argument("--anomaly-rate", type=float, default=0.15, help="Probability of discrepancy anomalies (0.0 - 1.0)")
    args = parser.parse_args()

    run_mes_simulator(rate_sec=args.rate, anomaly_rate=args.anomaly_rate, fmt=args.format)
