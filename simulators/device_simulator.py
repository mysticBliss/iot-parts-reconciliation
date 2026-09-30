#!/usr/bin/env python3
"""
Device Telemetry Simulator (MQTT Publisher)
Simulates 10 factory machines across 3 production lines:
 - Line 1 (Engine Block): pi-01, pi-02, pi-03, pi-04
 - Line 2 (Transmission): pi-05, pi-06, pi-07
 - Line 3 (Final Assembly): pi-08, pi-09, pi-10

Supports both compact delimited format (<device_id>::<line>::<parts>::<status>::<ts>) and JSON.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

_configured_host = os.getenv("MQTT_HOST", "localhost")
MQTT_HOST = "localhost" if _configured_host == "mosquitto" else _configured_host
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

# Multi-line Factory Topology
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

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"[MQTT] Connected successfully to broker at {MQTT_HOST}:{MQTT_PORT}", flush=True)
    else:
        print(f"[MQTT] Connection failed with code {rc}", flush=True)

def format_payload(dev, line, parts, status, ts, fmt="delimited"):
    if fmt == "delimited":
        # Format: <device_id>::<line>::<parts>::<status>::<ts>
        return f"{dev}::{line}::{parts}::{status}::{ts}"
    else:
        return json.dumps({
            "device_id": dev,
            "line": line,
            "parts": parts,
            "status": status,
            "ts": ts
        })

def run_simulator(rate_sec=1.5, outage_device=None, outage_duration=0, fmt="delimited"):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect

    print(f"Connecting to MQTT broker {MQTT_HOST}:{MQTT_PORT}...", flush=True)
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_start()
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}", flush=True)
        print("Ensure Mosquitto container is running: docker compose up -d mosquitto", flush=True)
        sys.exit(1)

    print(f"Starting simulation for {len(MACHINE_TOPOLOGY)} machines across 3 lines (Format: {fmt.upper()})...", flush=True)
    if outage_device:
        print(f"[WARN] Simulation will pause {outage_device} for {outage_duration}s and burst late data afterward.", flush=True)

    paused_backlog = []
    outage_start = None
    outage_active = False

    if outage_device:
        outage_trigger_time = time.time() + 10

    try:
        while True:
            current_time = time.time()

            # Handle scheduled outage for late-data testing
            if outage_device and not outage_active and current_time >= outage_trigger_time:
                outage_active = True
                outage_start = current_time
                print(f"\n[OUTAGE SIMULATION] Machine {outage_device} lost power/network connection!", flush=True)

            for dev, line in MACHINE_TOPOLOGY.items():
                ts = get_iso_timestamp()
                status = "RUNNING"
                parts = 1

                topic = f"factory/{line}/{dev}/parts"

                # If this device is experiencing an outage, buffer in RAM
                if outage_active and dev == outage_device:
                    status = "OUTAGE"
                    payload_str = format_payload(dev, line, parts, status, ts, fmt=fmt)
                    paused_backlog.append((topic, payload_str))
                    print(f"  [BUFFERED] {dev} ({line}) produced unit (Buffered in machine RAM: {len(paused_backlog)})", flush=True)
                else:
                    payload_str = format_payload(dev, line, parts, status, ts, fmt=fmt)
                    client.publish(topic, payload_str, qos=1)
                    print(f"  [PUBLISHED] {topic} -> {payload_str}", flush=True)

            # Check if outage duration finished and flush late backlog
            if outage_active and (current_time - outage_start) >= outage_duration:
                outage_active = False
                outage_device = None
                print(f"\n[RECOVERY] Machine recovered! Replaying {len(paused_backlog)} late events to broker...", flush=True)
                for topic, payload_str in paused_backlog:
                    client.publish(topic, payload_str, qos=1)
                    print(f"  [LATE-REPLAY] {topic} -> {payload_str}", flush=True)
                    time.sleep(0.05)
                paused_backlog.clear()

            time.sleep(rate_sec)

    except KeyboardInterrupt:
        print("\nStopping MQTT device simulator...", flush=True)
    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected cleanly.", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate IoT Machine telemetry via MQTT")
    parser.add_argument("--rate", type=float, default=2.0, help="Interval in seconds between production ticks")
    parser.add_argument("--format", type=str, choices=["delimited", "json"], default="delimited", help="Payload format (delimited '::' or json)")
    parser.add_argument("--simulate-outage-device", type=str, default=None, help="Device ID to simulate power outage on (e.g. pi-03)")
    parser.add_argument("--outage-duration", type=int, default=70, help="Duration of outage in seconds")
    args = parser.parse_args()

    run_simulator(
        rate_sec=args.rate,
        outage_device=args.simulate_outage_device,
        outage_duration=args.outage_duration,
        fmt=args.format
    )
