#!/usr/bin/env python3
"""
Dual Simulator Runner
Runs both the Edge Device (MQTT) Simulator and the MES (Cloud System of Record) Simulator
concurrently in two separate threads within a single process.
Supports clean baseline (clean mode with 0 glitches) and chaos/fault injection mode.
"""

import sys
import time
import argparse
import threading

try:
    from simulators.device_simulator import run_simulator as run_device_sim
    from simulators.mes_simulator import run_mes_simulator as run_mes_sim
except ImportError:
    from device_simulator import run_simulator as run_device_sim
    from mes_simulator import run_mes_simulator as run_mes_sim

def main():
    parser = argparse.ArgumentParser(description="Run Edge MQTT and Cloud MES simulators concurrently in threads.")
    parser.add_argument("--rate", type=float, default=2.0, help="Interval in seconds between ticks (default: 2.0)")
    parser.add_argument("--format", type=str, choices=["delimited", "json"], default="delimited", help="Payload format ('delimited' or 'json')")
    parser.add_argument("--clean", action="store_true", help="CLEAN MODE: Deactivates all glitches, schedule, and MES noise (Zero discrepancies baseline)")
    parser.add_argument("--no-schedule", action="store_true", help="Deactivate only the automated hourly timetable glitches")
    parser.add_argument("--glitch-rate", type=float, default=0.0, help="Probability of random edge hardware glitches (default: 0.0, e.g. 0.10)")
    parser.add_argument("--anomaly-rate", type=float, default=0.15, help="MES anomaly rate for discrepancy generation (default: 0.15, set 0 in --clean)")
    parser.add_argument("--simulate-outage-device", type=str, default=None, help="Device ID to simulate power outage on (e.g. pi-03)")
    parser.add_argument("--outage-duration", type=int, default=70, help="Duration of outage in seconds (default: 70)")
    args = parser.parse_args()

    # If --clean is passed, deactivate all faults
    if args.clean:
        scheduled_mode = False
        glitch_rate = 0.0
        anomaly_rate = 0.0
        mode_label = "CLEAN BASELINE (0 Glitches, Perfect Reconciliation)"
    else:
        scheduled_mode = not args.no_schedule
        glitch_rate = args.glitch_rate
        anomaly_rate = args.anomaly_rate
        mode_label = f"FAULT INJECTION (Schedule: {'ON' if scheduled_mode else 'OFF'}, Glitch: {glitch_rate*100:.0f}%, MES Anomaly: {anomaly_rate*100:.0f}%)"

    print("=================================================================", flush=True)
    print("🚀 Starting Dual Simulator (Edge MQTT + MES Cloud System of Record)", flush=True)
    print(f"⚙️  Mode: {mode_label} | Rate: {args.rate}s | Format: {args.format}", flush=True)
    print("=================================================================", flush=True)

    # Thread 1: Edge MQTT Device Simulator
    device_thread = threading.Thread(
        target=run_device_sim,
        kwargs={
            "rate_sec": args.rate,
            "outage_device": args.simulate_outage_device,
            "outage_duration": args.outage_duration,
            "fmt": args.format,
            "glitch_rate": glitch_rate,
            "scheduled_mode": scheduled_mode
        },
        name="Device-Simulator-Thread",
        daemon=True
    )

    # Thread 2: MES Cloud Simulator
    mes_thread = threading.Thread(
        target=run_mes_sim,
        kwargs={
            "rate_sec": args.rate,
            "anomaly_rate": anomaly_rate,
            "fmt": args.format
        },
        name="MES-Simulator-Thread",
        daemon=True
    )

    device_thread.start()
    mes_thread.start()

    try:
        while device_thread.is_alive() and mes_thread.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n🛑 Stopping dual simulator threads...", flush=True)
        sys.exit(0)

if __name__ == "__main__":
    main()
