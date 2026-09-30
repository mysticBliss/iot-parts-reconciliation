#!/usr/bin/env python3
"""
Device Telemetry Simulator (MQTT Publisher)
Simulates 10 factory machines across 3 production lines.
Supports:
 - Predictable hourly / cycle schedule of glitches (or random).
 - Scheduled network outages & late burst replay.
"""

import os
import sys
import json
import time
import random
import argparse
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

_configured_host = os.getenv("MQTT_HOST", "localhost")
MQTT_HOST = "localhost" if _configured_host == "mosquitto" else _configured_host
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

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

# PREDICTABLE HOURLY TIMETABLE
# Format: Minute range of the hour -> Fault Scenario
HOURLY_FAULT_TIMETABLE = {
    "MM:05 - MM:08": ("pi-02 (line1)", "Sensor Double-Bounce (Parts: 2-3)", "Vibrational chatter / Reflective bounce"),
    "MM:15 - MM:18": ("pi-06 (line2)", "Missed Pulse (Parts: 0)", "Optical lens fogging / Conveyor slip"),
    "MM:25 - MM:27": ("pi-09 (line3)", "Clock Jitter (+/- 25s)", "PLC NTP desynchronization"),
    "MM:35 - MM:37": ("pi-03 (line1)", "Mini Network Flap (Outage + Burst)", "Intermittent factory Wi-Fi drop"),
    "MM:48 - MM:51": ("pi-07 (line2)", "Burst Double-Bounce (Parts: 2)", "Overheated sensor switch bounce")
}

def print_hourly_schedule_manifest():
    print("\n📋 ═══════════════════════════════════════════════════════════════════════════════", flush=True)
    print("   AUTOMATED HOURLY GLITCH & ANOMALY SCHEDULE (For Superset Dashboard Tracking)", flush=True)
    print("═══════════════════════════════════════════════════════════════════════════════════", flush=True)
    for time_window, (target, anomaly_type, root_cause) in HOURLY_FAULT_TIMETABLE.items():
        print(f"  🕒 Window [{time_window}] ➜ Target: {target:18} | Type: {anomaly_type:30} | Cause: {root_cause}", flush=True)
    print("═══════════════════════════════════════════════════════════════════════════════════\n", flush=True)

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
        return f"{dev}::{line}::{parts}::{status}::{ts}"
    else:
        return json.dumps({
            "device_id": dev,
            "line": line,
            "parts": parts,
            "status": status,
            "ts": ts
        })

def run_simulator(rate_sec=2.0, outage_device=None, outage_duration=70, fmt="delimited", glitch_rate=0.0, scheduled_mode=True):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="factory-fleet-simulator")
    client.on_connect = on_connect

    print(f"Connecting to MQTT broker {MQTT_HOST}:{MQTT_PORT}...", flush=True)
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_start()
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}", flush=True)
        print("Ensure Mosquitto container is running: docker compose up -d mosquitto", flush=True)
        sys.exit(1)

    if scheduled_mode:
        print_hourly_schedule_manifest()

    paused_backlog = []
    outage_start = None
    outage_active = False

    if outage_device:
        outage_trigger_time = time.time() + 10

    try:
        while True:
            current_time = time.time()
            now_dt = datetime.now(timezone.utc)
            curr_minute = now_dt.minute

            # Scheduled Outage from timetable (MM:35 to MM:37 on pi-03) or manual flag
            active_outage_target = outage_device
            if scheduled_mode and curr_minute in [35, 36]:
                active_outage_target = "pi-03"

            if active_outage_target and not outage_active:
                if (outage_device and current_time >= outage_trigger_time) or (scheduled_mode and curr_minute in [35, 36]):
                    outage_active = True
                    outage_start = current_time
                    print(f"\n[OUTAGE SIMULATION] Machine {active_outage_target} network dropped (Buffering in RAM)...", flush=True)

            for dev, line in MACHINE_TOPOLOGY.items():
                ts_offset = 0
                status = "RUNNING"
                parts = 1
                is_glitch = False
                glitch_name = ""

                # 1. Check Automated Hourly Timetable
                if scheduled_mode:
                    if curr_minute in [5, 6, 7] and dev == "pi-02":
                        parts = random.choice([2, 3])
                        is_glitch = True
                        glitch_name = f"SCHEDULED DOUBLE-BOUNCE (Parts: {parts})"
                    elif curr_minute in [15, 16, 17] and dev == "pi-06":
                        parts = 0
                        is_glitch = True
                        glitch_name = "SCHEDULED MISSED-PULSE (Parts: 0)"
                    elif curr_minute in [25, 26] and dev == "pi-09":
                        ts_offset = random.choice([-25, 25])
                        is_glitch = True
                        glitch_name = f"SCHEDULED CLOCK-JITTER ({ts_offset:+d}s)"
                    elif curr_minute in [48, 49, 50] and dev == "pi-07":
                        parts = 2
                        is_glitch = True
                        glitch_name = "SCHEDULED DOUBLE-BOUNCE (Parts: 2)"

                # 2. Random Glitch Injection (if scheduled wasn't triggered)
                if not is_glitch and glitch_rate > 0 and random.random() < glitch_rate:
                    is_glitch = True
                    glitch_type = random.choices(["bounce", "missed_pulse", "clock_jitter"], weights=[0.5, 0.3, 0.2])[0]
                    if glitch_type == "bounce":
                        parts = random.choice([2, 3])
                        glitch_name = f"RANDOM DOUBLE-BOUNCE (Parts: {parts})"
                    elif glitch_type == "missed_pulse":
                        parts = 0
                        glitch_name = "RANDOM MISSED-PULSE (Parts: 0)"
                    elif glitch_type == "clock_jitter":
                        ts_offset = random.choice([-25, 25])
                        glitch_name = f"RANDOM CLOCK-JITTER ({ts_offset:+d}s)"

                ts = get_iso_timestamp(offset_seconds=ts_offset)
                topic = f"factory/{line}/{dev}/parts"

                # Handle Outage Buffering
                if outage_active and dev == active_outage_target:
                    status = "OUTAGE"
                    payload_str = format_payload(dev, line, parts, status, ts, fmt=fmt)
                    paused_backlog.append((topic, payload_str))
                    print(f"  [BUFFERED] {dev} ({line}) produced unit (Buffered: {len(paused_backlog)})", flush=True)
                else:
                    payload_str = format_payload(dev, line, parts, status, ts, fmt=fmt)
                    client.publish(topic, payload_str, qos=1)

                    if is_glitch:
                        print(f"  ⚡ [{glitch_name}] {topic} -> {payload_str}", flush=True)
                    else:
                        print(f"  [PUBLISHED] {topic} -> {payload_str}", flush=True)

            # Check Outage Recovery
            if outage_active:
                outage_done = False
                if outage_device and (current_time - outage_start) >= outage_duration:
                    outage_done = True
                elif scheduled_mode and curr_minute not in [35, 36]:
                    outage_done = True

                if outage_done:
                    outage_active = False
                    print(f"\n[RECOVERY] Machine recovered! Flushing {len(paused_backlog)} buffered late events to broker...", flush=True)
                    for topic, payload_str in paused_backlog:
                        client.publish(topic, payload_str, qos=1)
                        print(f"  [LATE-REPLAY] {topic} -> {payload_str}", flush=True)
                        time.sleep(0.04)
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
    parser.add_argument("--glitch-rate", type=float, default=0.0, help="Probability of random hardware glitches (0.0 to 1.0)")
    parser.add_argument("--no-schedule", action="store_true", help="Disable the automated hourly glitch timetable")
    parser.add_argument("--simulate-outage-device", type=str, default=None, help="Device ID to simulate manual power outage on (e.g. pi-03)")
    parser.add_argument("--outage-duration", type=int, default=70, help="Duration of manual outage in seconds")
    args = parser.parse_args()

    run_simulator(
        rate_sec=args.rate,
        outage_device=args.simulate_outage_device,
        outage_duration=args.outage_duration,
        fmt=args.format,
        glitch_rate=args.glitch_rate,
        scheduled_mode=not args.no_schedule
    )
