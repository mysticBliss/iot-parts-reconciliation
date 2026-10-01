# IoT Edge to Cloud Reconciliation Engine
### Real-Time Discrepancy Detection with MQTT, Self-Managed Kafka Connect, Confluent Cloud, Flink SQL & Streamlit

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/)
![Architecture](https://img.shields.io/badge/Architecture-Event--Driven-blue.svg)
![Confluent](https://img.shields.io/badge/Confluent%20Cloud-Kafka%20%7C%20Flink%20SQL-black.svg)
![Status](https://img.shields.io/badge/Status-Live%20Demo%20Ready-brightgreen.svg)

> **Documentation:** [mysticbliss.github.io/iot-parts-reconciliation](https://mysticbliss.github.io/iot-parts-reconciliation/) — the build story, every SQL file explained, fault ground truth, concepts
>
> **Dashboard:** [iot-parts-reconciliation.streamlit.app](https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/) — replays the fault timetable; see the docs for how it differs from the live pipeline

A stream processing pipeline that reconciles high-velocity IoT edge manufacturing counts against an enterprise Manufacturing Execution System (MES) system of record in real time.


---

## 1. Problem Statement & Background

In discrete manufacturing plants (such as automotive assembly lines), edge sensors on production machinery emit physical piece-count pulses over MQTT, while an enterprise MES (System of Record) records batch completions. 

Historically (e.g. Delphi 2018), telemetry reconciliation was performed using edge MiNiFi agents, NiFi routing clusters, and micro-batch Hortonworks pipelines. This project modernizes that pattern to a cloud-native, event-driven streaming stack using **Confluent Cloud** and **Apache Flink SQL**:
- Reconciles edge counts vs. system counts over **1-minute tumbling windows**.
- Detects dropped pulses, multi-count miscounts, and clock drifts.
- Measures how edge network drops and buffered replay behave against the windowing clock — see §5A, which documents why windowing on Kafka ingestion time surfaces an outage as a recovery-window burst rather than as late data.

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
 (bronze: raw delimited, schemaless)                                         (MES System of Record Publisher)
       │                                                                                  │
       └────────────────────────────────────────┬─────────────────────────────────────────┘
                                                ▼
                                 [Confluent Cloud Flink SQL]
                       - 1-Minute Tumbling Window on $rowtime (ingestion time)
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
├── LEARNING.md                      # Pointer — the deep dives now live in docs/
├── mkdocs.yml, docs/                # Documentation site (published to GitHub Pages)
├── connect/
│   └── Dockerfile                   # Connect worker image with the MQTT source plugin
├── mosquitto/
│   └── config/mosquitto.conf        # Mosquitto broker configuration
├── connectors/
│   ├── mqtt-source-connector.json   # MQTT -> Confluent Cloud, raw bytes (deployed)
│   └── mqtt-source-avro-connector.json # Avro alternative — reference only, needs a parsing SMT to run
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

### A. Ingestion Time vs Event Time — what the outage test actually showed (Phase 6)

This is the most useful thing the project taught me, and it is not what I expected to find.

**Setup.** Simulate a plant network drop (`python simulators/device_simulator.py --simulate-outage-device pi-03 --outage-duration 70`): `pi-03` goes offline for 70 seconds, buffers its pulses, and flushes them on reconnect.

**Expectation.** The replayed events would arrive after the watermark had advanced past their window, Flink would discard them, and the reconciliation would show a deficit for the outage minutes.

**What actually happened.** A surplus in the *recovery* window, not a deficit — classified by [02_windowed_reconciliation.sql](./flink_sql/02_windowed_reconciliation.sql) as `BURST_RECOVERY`.

**Why.** Every window in this project is keyed on `$rowtime` (see [01_create_tables.sql](./flink_sql/01_create_tables.sql)). Per Confluent's documentation, `$rowtime` *"is exactly the Kafka record timestamp"* — and with no timestamp SMT on the MQTT source connector, that timestamp is assigned by the Connect worker **when it publishes the record**, not when the device generated the pulse. The device's own clock reading travels in the payload as the string field `ts` and is never promoted to a time attribute.

So on replay, the buffered events are stamped with *current* publish time. They are not late. They are perfectly punctual records carrying stale readings, and they aggregate into the minute the device reconnected.

**The tradeoff I thought I was making, and the one I was actually making.** Widening the watermark would have changed nothing here — at any tolerance, those records are never late. The real distinction is that this is an **ingestion-time pipeline**, and ingestion time is cheap, monotonic and immune to device clock drift, at the cost of attributing late-arriving work to the wrong window. Event time would attribute it correctly, at the cost of holding window state open and trusting edge clocks.

**Watermark configuration, stated plainly.** These tables run Confluent Cloud's **default** watermark strategy: applied on `$rowtime`, calculated per Kafka partition, with a fixed out-of-orderness tolerance of **180 milliseconds**. No custom watermark is declared anywhere in `flink_sql/`. Confluent exposes watermarks at table level, so tuning one here would mean:

```sql
ALTER TABLE device_counts
  MODIFY WATERMARK FOR $rowtime AS $rowtime - INTERVAL '10' SECOND;
```

**What I would change to get true event-time semantics.** Parse `ts` into a `TIMESTAMP(3)`, declare it as the time attribute with a watermark sized to the worst tolerable edge outage, and window on that instead of `$rowtime`. Then late data genuinely crosses a watermark boundary, the deficit appears where I originally expected it, and the lag views in [06_streamlit_analytics_views.sql](./flink_sql/06_streamlit_analytics_views.sql) — which already measure the gap between `ts` and `$rowtime` — become the signal for sizing that watermark. Alternatively, a `TimestampConverter` SMT on the source connector would push the device clock into the Kafka record timestamp and leave the SQL unchanged.

### C. Where the schema contract lives

The edge payload is a `::`-delimited string — the same wire shape the original Delphi pipeline carried through MiNiFi and NiFi, kept so the two implementations compare on equal terms.

**The edge is schemaless on purpose.** Confluent's Avro wire format puts a magic byte and a 4-byte schema ID on every record, which means the producer needs a Schema Registry connection. On machine-mounted sensors that means registry credentials at the plant edge, OT-to-cloud egress and firmware coupled to schema IDs — a poor trade for a device fleet.

So the contract is enforced one hop later, which makes this medallion architecture on streams:

- **Bronze** — `device_counts` and `system_counts` carry no schema. Confluent Cloud Flink infers them as `key VARBINARY, val VARBINARY` with `'value.format' = 'raw'`, so nothing at the edge can break ingestion by changing shape.
- **Silver / gold** — `count_mismatches` is a declared `CREATE TABLE`, and per Confluent's documentation *"The CREATE TABLE statement always creates a backing Kafka topic as well as the corresponding schema subjects for key and value in Schema Registry."* Downstream consumers bind to that typed contract.

The cost is that parsing lives in SQL: `MAKE_VALID_UTF8()` and `SPLIT_INDEX()` repeated across the views, with no type safety on the raw hop.

**One thing that did not work, recorded because the reason matters.** A schema was registered against the raw `device_counts` subject expecting Flink to return typed columns. It stayed inert — Schema Registry is a producer-side serialization contract, not a parser, and `ByteArrayConverter` writes the payload through with no schema ID for a deserializer to key off. Swapping in `AvroConverter` alone would not fix it either: it serializes whatever structure the Connect record already has, and the MQTT source hands it a byte array, so the registered schema would be a *primitive* and Flink would return one `STRING` column holding the whole delimited payload. Structuring at the connector needs a custom SMT. [part_event.avsc](./schemas/part_event.avsc) and [mqtt-source-avro-connector.json](./connectors/mqtt-source-avro-connector.json) remain as the device-side alternative, for deployments where edge devices can legitimately hold registry credentials.

### B. Self-Managed Connect vs Managed Connect
- **Challenge:** Confluent Cloud Managed Connectors cannot reach local or on-premise private edge brokers without complex VPC peering or reverse proxy tunnels.
- **Solution:** Running distributed self-managed Connect workers locally provides a secure outbound TLS pipe into Confluent Cloud while preserving full control over Single Message Transforms (SMTs) and Dead Letter Queues (DLQs).

---

## 6. Author
- **Saqib Mujtaba** — Senior Big Data / Streaming Engineer
- Portfolio: [github.com/mysticBliss](https://github.com/mysticBliss)
