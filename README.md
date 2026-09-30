# IoT Edge to Cloud Reconciliation Engine
### Real-Time Discrepancy Detection with MQTT, Self-Managed Kafka Connect, Confluent Cloud, Flink SQL & Streamlit

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/)
![Architecture](https://img.shields.io/badge/Architecture-Event--Driven-blue.svg)
![Confluent](https://img.shields.io/badge/Confluent%20Cloud-Kafka%20%7C%20Flink%20SQL-black.svg)
![Status](https://img.shields.io/badge/Status-Live%20Demo%20Ready-brightgreen.svg)

> **Live Interactive Cloud Dashboard:** [https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/](https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/)

A stream processing pipeline that reconciles high-velocity IoT edge manufacturing counts against an enterprise Manufacturing Execution System (MES) system of record in real time.


---

## 1. Problem Statement & Background

In discrete manufacturing plants (such as automotive assembly lines), edge sensors on production machinery emit physical piece-count pulses over MQTT, while an enterprise MES (System of Record) records batch completions. 

Historically (e.g. Delphi 2018), telemetry reconciliation was performed using edge MiNiFi agents, NiFi routing clusters, and micro-batch Hortonworks pipelines. This project modernizes that pattern to a cloud-native, event-driven streaming stack using **Confluent Cloud** and **Apache Flink SQL**:
- Reconciles edge counts vs. system counts over **1-minute tumbling windows**.
- Detects dropped pulses, multi-count miscounts, and clock drifts.
- Evaluates the impact of edge device network drops and out-of-order event replay against Flink event-time watermarks.

---

## 2. Architecture & Data Flow

```
                                      [Edge Machine Layer]
                     10x Simulated Raspberry Pi Devices (paho-mqtt)
                                                │
                                         (MQTT QoS 1)
                                                ▼
                                    [Eclipse Mosquitto Broker]
                                                │
                                                ▼
                           [Self-Managed Kafka Connect Worker]
                           (Distributed Mode, SASL_SSL to Cloud)
                                                │
       ┌────────────────────────────────────────┴────────────────────────────────────────┐
       ▼                                                                                  ▼
[Confluent Cloud: device_counts]                                            [Confluent Cloud: system_counts]
 (Avro / Schema Registry enforced)                                           (MES System of Record Publisher)
       │                                                                                  │
       └────────────────────────────────────────┬─────────────────────────────────────────┘
                                                ▼
                                 [Confluent Cloud Flink SQL]
                       - 1-Minute Tumbling Event-Time Window Aggregation
                       - Stream-to-Stream Discrepancy Join (device_total != system_total)
                                                │
                                                ▼
                              [Confluent Cloud: count_mismatches]
                                                │
                                                ▼
                                  [Streamlit Analytics Dashboard]
                             (deployed on Streamlit Community Cloud)
```

> **On the dashboard.** Keeping a Confluent Cloud cluster and a local Connect worker
> running continuously is not practical for a public demo, so the hosted dashboard does
> not hold an open Kafka consumer. It renders the same reconciliation output the Flink
> SQL views in [flink_sql/](./flink_sql/) produce, reproduced from the simulators'
> deterministic fault timetable (documented in [CASE_STUDY.md](./CASE_STUDY.md) §5). The stream processing is real; the hosted page
> is a verification surface for it. Run the stack locally and the dashboard shows your
> own cluster's output.

---

## 3. Project Structure

```
iot-parts-reconciliation/
├── .env.example                     # Environment template (Confluent Cloud credentials)
├── docker-compose.yml               # Local infra (Mosquitto + self-managed Kafka Connect)
├── requirements.txt                 # Python dependencies
├── dashboard.py                     # Streamlit reconciliation dashboard
├── architecture.drawio              # HLD and LLD diagrams
├── CASE_STUDY.md                    # Engineering case study — decisions, tradeoffs, results
├── LEARNING.md                      # Deep dives: MATCH_RECOGNIZE, watermarks, dashboard design
├── connect/
│   └── Dockerfile                   # Connect worker image with the MQTT source plugin
├── mosquitto/
│   └── config/mosquitto.conf        # Mosquitto broker configuration
├── connectors/
│   ├── mqtt-source-connector.json   # MQTT -> Confluent Cloud (JSON mode)
│   └── mqtt-source-avro-connector.json # MQTT -> Confluent Cloud (Avro mode)
├── schemas/
│   └── part_event.avsc              # Avro schema for Schema Registry evolution
├── simulators/
│   ├── device_simulator.py          # Edge MQTT publisher (supports outage injection)
│   ├── mes_simulator.py             # MES system-of-record publisher with deliberate noise
│   └── dual_simulator.py            # Runs both sides against the hourly fault timetable
├── flink_sql/
│   ├── 01_create_tables.sql         # Flink views over the raw topics
│   ├── 02_windowed_reconciliation.sql # Tumbling window join + discrepancy classification
│   ├── 03_watermark_experiments.sql # Late data, clock drift & watermark tolerance
│   ├── 04_temporal_enrichment.sql   # Dimension enrichment (line, operation, valuation)
│   ├── 05_complex_event_processing.sql # MATCH_RECOGNIZE pattern detection
│   └── 06_streamlit_analytics_views.sql # Analytics views feeding the dashboard
└── scripts/
    ├── create_topics.py             # Topic creation against Confluent Cloud
    ├── deploy_connectors.ps1        # PowerShell connector deployment
    ├── deploy_connectors.sh         # Bash connector deployment
    ├── deploy_connectors.py         # Python connector deployment
    └── reset_cluster.py             # Tear down topics and connectors between runs
```

The repository also carries `postgres/init.sql` and `connectors/jdbc-sink-connector.json`
from the earlier Postgres-and-Superset design. They are **not part of the current
stack** — `docker-compose.yml` no longer starts Postgres, and the reconciliation output
is read straight from Confluent Cloud rather than persisted relationally.

---

## 4. Quick Start & Execution Guide

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- A free Confluent Cloud cluster with Schema Registry enabled

### Step 1: Clone & Configure Environment
```bash
cp .env.example .env
# Edit .env with your Confluent Cloud Bootstrap Server, API Keys, and Schema Registry URL
```

### Step 2: Install Python Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Start Local Infrastructure
```bash
docker compose up -d --build
```
This boots:
- Mosquitto on `localhost:1883`
- Self-managed Kafka Connect on `localhost:8083` (connected via SASL_SSL to Confluent Cloud)

### Step 4: Deploy Kafka Connect Connectors
```powershell
# On Windows PowerShell:
cd scripts
.\deploy_connectors.ps1
```

### Step 5: Run Edge & MES Simulators
In Terminal 1 (Edge Machine telemetry via MQTT):
```bash
python simulators/device_simulator.py --rate 1.5
```

In Terminal 2 (MES System of Record directly to Confluent Cloud):
```bash
python simulators/mes_simulator.py --rate 1.5 --anomaly-rate 0.15
```

Or drive both sides from one process, on the reproducible hourly fault timetable:
```bash
python simulators/dual_simulator.py
```

### Step 6: Execute Flink SQL in Confluent Cloud Workspace
1. Run `flink_sql/01_create_tables.sql` to register the `device_counts`, `system_counts`
   and `count_mismatches` views.
2. Run `flink_sql/02_windowed_reconciliation.sql` to start continuous windowed reconciliation.
3. Optionally run `04`–`06` for dimension enrichment, `MATCH_RECOGNIZE` pattern detection,
   and the analytics views the dashboard is shaped around.

### Step 7: Launch the Dashboard
```bash
streamlit run dashboard.py
```

---

## 5. Engineering Findings & Tradeoffs

### A. Late Data & Watermark Boundary Behavior (Phase 6 Experiment)
During simulated plant power outages (`python simulators/device_simulator.py --simulate-outage-device pi-03 --outage-duration 70`):
- When machine `pi-03` dropped offline for 70 seconds and subsequently flushed its RAM buffer, events arrived with event timestamps falling outside Flink's 10-second bounded watermark (`WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND`).
- **Observation:** Late-arriving events were discarded by Flink's stream join, resulting in apparent reconciliation deficits.
- **Resolution & Tradeoff:** Increasing watermark tolerance to `60 SECONDS` prevented dropped records at the expense of keeping stream state in memory for an additional minute before emitting finalized window results.

### B. Self-Managed Connect vs Managed Connect
- **Challenge:** Confluent Cloud Managed Connectors cannot reach local or on-premise private edge brokers without complex VPC peering or reverse proxy tunnels.
- **Solution:** Running distributed self-managed Connect workers locally provides a secure outbound TLS pipe into Confluent Cloud while preserving full control over Single Message Transforms (SMTs) and Dead Letter Queues (DLQs).

---

## 6. Author
- **Saqib Mujtaba** — Senior Big Data / Streaming Engineer
- Portfolio: [github.com/mysticBliss](https://github.com/mysticBliss)
