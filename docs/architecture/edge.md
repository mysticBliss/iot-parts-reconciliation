---
description: "Simulated plant topology, the ::-delimited MQTT payload, QoS 1, outage buffering, and the MES system-of-record simulator."
---

# Edge devices & MQTT

## Plant topology

Ten simulated Raspberry Pis across three lines (`simulators/device_simulator.py`):

| Line | Devices | Role in the fault timetable |
|---|---|---|
| `line1` | `pi-01`, `pi-02`, `pi-03`, `pi-04` | `pi-02` bounces at :05–:07 · `pi-03` outage at :35–:36 |
| `line2` | `pi-05`, `pi-06`, `pi-07` | `pi-06` misses pulses at :15–:17 · `pi-07` double-counts at :48–:50 |
| `line3` | `pi-08`, `pi-09`, `pi-10` | `pi-09` clock jitter at :25–:26 |

Every tick (default 2 s) each device publishes one pulse — so 30 pulses per device per
minute in normal operation.

## Payload

```
<device_id>::<line>::<parts>::<status>::<ts>
pi-03::line1::1::RUNNING::2026-09-30T19:35:02Z
```

| Field | Meaning |
|---|---|
| `parts` | Usually `1`. `2`–`3` for a bounce, `0` for a missed pulse |
| `status` | `RUNNING`, or `OUTAGE` for pulses produced while buffered |
| `ts` | The device's own clock, UTC, second precision. Offset ±25 s during jitter |

`--format json` switches both simulators to JSON with the same fields. The SQL only
parses the delimited form.

## MQTT

- **Broker:** Eclipse Mosquitto 2.0, listener on `1883`, anonymous access, persistence on.
- **Topic per device:** `factory/<line>/<device>/parts`. The Connect worker subscribes to
  `factory/#`.
- **QoS 1** — at-least-once delivery between device and broker. Duplicates are possible on
  retry; the pipeline doesn't deduplicate.

!!! warning "Not production settings"
    `allow_anonymous true` and a plaintext listener are fine on a laptop. A plant broker
    needs TLS and per-device credentials — those are plant-network credentials, though,
    not cloud ones, which is the point of the design.

## Outage buffering

When a device is "offline" it keeps producing, tags each pulse `status = OUTAGE`, and
appends it to an in-memory list. On recovery the list is published in order, 40 ms
apart. Each buffered pulse keeps the `ts` from when it was produced — which is why the
lag metric can spot a replay, and why [Chapter 3](../story/outage.md) happened.

| Trigger | When | Duration |
|---|---|---|
| Scheduled | Minutes :35 and :36 of every hour, on `pi-03` | Until minute :37 starts |
| Manual | `--simulate-outage-device pi-03`, 10 s after start | `--outage-duration`, default 70 s |

## The MES side

`simulators/mes_simulator.py` stands in for the system of record. It publishes the
*expected* count for every device on the same tick, directly to `system_counts` with a
Kafka producer keyed by `device_id`.

It adds random noise (`--anomaly-rate`, default 0.15), split into dropped records,
timing drift and multi-counts. Timing drift only changes `ts` — see
[Chapter 5](../story/silent-faults.md#mes-timing-drift). `--clean` on the dual simulator
turns all faults and noise off for a zero-discrepancy baseline.
