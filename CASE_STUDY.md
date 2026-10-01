# CASE STUDY: IoT Edge-to-Cloud Real-Time Reconciliation Engine

> **Full documentation:** [mysticbliss.github.io/iot-parts-reconciliation](https://mysticbliss.github.io/iot-parts-reconciliation/)
> — the story of the build, every SQL file explained, the fault ground-truth matrix, and
> the concepts behind it. This page is the short version.

## Quick Facts
- **Role:** Lead Data / Streaming Architect & Engineer
- **Domain:** Industrial IoT, Discrete Manufacturing (Automotive Assembly)
- **Tech Stack:** Confluent Cloud (Kafka, Flink SQL, Schema Registry), Apache Kafka Connect, Eclipse Mosquitto (MQTT), Streamlit, Docker, Python
- **Key Outcome:** Continuous discrepancy detection between distributed edge machinery and enterprise MES records within 1-minute tumbling windows, replacing a retrospective batch reconciliation cycle.
- **Two design choices worth knowing up front:** the edge payload is schemaless by intent, with the registered schema contract applied at the Flink boundary rather than at the device (§4A); and windows run on `$rowtime`, Kafka ingestion time, under Confluent's default watermark, which shapes how a device outage surfaces (§4C).

---

## 1. Project Overview

In discrete manufacturing facilities, piece-count telemetry emitted by hardware sensors on physical assembly lines must match batch completion records tracked in the enterprise Manufacturing Execution System (MES). 

This project delivers a **cloud-native, event-driven streaming pipeline** that continuously ingests physical IoT pulse telemetry (MQTT), reconciles high-velocity edge production counts against cloud MES records using **Confluent Cloud Apache Flink SQL**, and publishes the result under a registered Schema Registry contract.

```
[Edge Machine Sensors] ──(MQTT)──▶ [Eclipse Mosquitto] ──▶ [Kafka Connect] ──▶ [Confluent Cloud Kafka]
                                                                                      │
                                  [Enterprise MES SOR] ────────(Direct Cloud)────────┤
                                                                                      ▼
                    [Streamlit Dashboard] ◀─────────────────── [Confluent Flink SQL Engine]
                                                                (1-Min Tumbling Window Reconciler)
```

---

## 2. The Problem & Business Context

Automotive and discrete manufacturing facilities face severe challenges reconciling physical output against recorded production numbers:

- **Silent Inventory Discrepancies:** Edge sensor pulses (from PLC/Raspberry Pi counters) are susceptible to network drops, clock drifts, and double-counts. Undetected mismatches between physical line output and ERP/MES batch completions lead to stock inaccuracies and costly line shutdowns.
- **Operational Weight:** The 2018 Delphi build reconciled in near real time, but on MiNiFi agents, a self-run MQTT cluster, NiFi flows and a Hortonworks cluster — a lot of infrastructure to operate for a few lines of reconciliation logic.
- **Unstructured Payloads & Schema Drift:** Without schema governance, raw edge JSON payloads frequently mutate, breaking downstream data pipelines and rendering analytics unreliable.

### Engineering Goals

These were the goals set at the start. Two of the three were met as stated; the third
turned into the most instructive part of the project. §4A and §4C record where each landed.

1. **Sub-minute Latency:** Detect edge-to-MES count mismatches within 1-minute tumbling
   windows. *Met.*
2. **Strict Schema Governance:** Enforce a registered schema contract rather than letting
   raw payloads flow downstream unchecked. *Met, but one hop later than originally
   intended — the contract is registered at the Flink boundary by `CREATE TABLE`, not at
   the device. §4A sets out why pushing Avro to the plant floor is the wrong trade.*
3. **Resilience to Edge Network Outages:** Handle out-of-order replay and late-arriving
   telemetry using Flink watermarks. *Not met as framed, and the reason is the finding:
   windowing on `$rowtime` means post-outage replay is never late — it is punctual data
   carrying stale readings, attributed to the recovery window. Watermarks were not the
   lever; the choice of time attribute was.*

---

## 3. Architecture & Technical Decisions

### Key Decisions Matrix

| Decision Area | Selected Approach | Alternatives Considered | Rationale |
| :--- | :--- | :--- | :--- |
| **Edge Protocol** | MQTT QoS 1 over Eclipse Mosquitto | Direct HTTP REST, CoAP | Lightweight edge footprint, guaranteed delivery, minimal battery/CPU consumption on edge devices. |
| **Edge-to-Cloud Bridge** | Self-Managed Kafka Connect Worker (Distributed) | Custom bridge script, Cloud-managed connector | Cost efficiency, SASL_SSL authentication to Confluent Cloud, low-latency connector plugin execution. |
| **Where the schema contract lives** | Schemaless `::` payload at the edge; typed schema registered at the Flink boundary via `CREATE TABLE` | Avro + Schema Registry at the device; a custom SMT structuring the payload at Connect | Avro's wire format requires the producer to hold a Schema Registry connection. On a fleet of machine-mounted sensors that means registry credentials at the plant edge, OT-to-cloud egress, and firmware coupled to schema IDs. Capturing raw and enforcing one hop later keeps devices dumb and ingestion unbreakable. The cost is parsing logic in SQL. |
| **Windowing Clock** | `$rowtime` (Kafka record timestamp) with Confluent's default watermark — per partition, 180 ms out-of-orderness | Declared event time on the payload's `ts` field; `TimestampConverter` SMT at the connector | Ingestion time is monotonic, needs no trust in edge device clocks, and required no extra work. The cost is that buffered replay after an outage is attributed to the recovery window instead of the window it belongs to — see §4C. |
| **Stream Processing Engine** | Confluent Cloud Flink SQL | Spark Streaming, ksqlDB | First-class tumbling window joins, declarative time attributes and watermarks, serverless auto-scaling on Confluent Cloud. |
| **Discrepancy Surface** | Flink analytics views read into a Streamlit dashboard | JDBC sink to PostgreSQL + Superset (built first), Elasticsearch, InfluxDB | The relational hop added a persistence tier and a second container stack without changing what the operator sees. Shaping the output in Flink views and rendering it directly removed Postgres and Superset from the runtime entirely. |

---

## 4. Deep-Dive Implementation

### A. Edge Telemetry, Serialization, and Where the Contract Lives

Edge simulators generate hardware part-pulse counts and publish them to Mosquitto. A Kafka Connect MQTT Source connector ingests them into Confluent Cloud.

**The edge payload is a `::`-delimited string, deliberately.** It is the same wire shape the original Delphi pipeline carried through MiNiFi and NiFi in 2018, kept so the two implementations could be compared on equal terms rather than on serialization differences.

**The edge stays schemaless on purpose.** Confluent's Avro wire format places a magic byte and a 4-byte schema ID on every record, which means the *producer* must hold a Schema Registry connection. Pushing that to the plant floor puts registry credentials on every device, opens egress from the OT network to a cloud service, and couples firmware to schema IDs. For a fleet of machine-mounted sensors that is a poor trade. The industry convention reflects this — MQTT device fleets speak Sparkplug B, JSON or a compact delimited form, and structure is applied at the ingestion boundary rather than at the device.

**So the contract lives at the Flink boundary, and it is a real one.** The architecture is medallion applied to streams:

| Layer | Topic | Schema | Why |
| :--- | :--- | :--- | :--- |
| Bronze | `device_counts`, `system_counts` | None — inferred as `key VARBINARY`, `val VARBINARY` with `'value.format' = 'raw'` | Lossless capture. Nothing at the edge can break ingestion by changing shape |
| Silver / Gold | `count_mismatches` | Declared, typed, registered | The contract downstream consumers actually bind to |

That second row is not aspirational. Per Confluent's documentation, *"The CREATE TABLE statement always creates a backing Kafka topic as well as the corresponding schema subjects for key and value in Schema Registry."* The `CREATE TABLE count_mismatches (...)` in [01_create_tables.sql](./flink_sql/01_create_tables.sql) therefore registers a value schema covering `window_start`, `window_end`, `device_id`, `device_total`, `system_total`, `discrepancy`, `issue_type` and `detected_at`. Schema enforcement in this pipeline is real; it is applied one hop later than a textbook diagram would put it.

The cost of that choice is visible and worth stating: the parsing lives in SQL. Every view opens with `MAKE_VALID_UTF8()` and a column of `SPLIT_INDEX()` calls, duplicated across six files, with no type safety on the bronze hop and nothing to stop a producer reordering fields.

#### A note on an experiment that did not work

Partway through, a schema was registered against the raw `device_counts` subject in the expectation that Flink would then hand back typed columns. It did not, and the reason is worth recording.

**Schema Registry is a producer-side serialization contract, not a parser.** The deployed connector uses `ByteArrayConverter`, which writes the MQTT payload through untouched — no magic byte, no schema ID. There is nothing in the record for a deserializer to key off, so the registered subject is inert: nothing validates against it and nothing decodes with it. The proof is in the SQL, which still reads a raw `val` column; had a schema been in force, the table would have exposed typed columns instead.

Swapping in `AvroConverter` alone would not fix it either. The converter serializes whatever structure the Connect record already carries, and the MQTT source hands it a byte array — no Struct, no fields. That registers a *primitive* schema, and Confluent's docs are explicit that a primitive yields a single typed column. The result would be one `STRING` column holding the whole delimited payload: `MAKE_VALID_UTF8()` disappears, `SPLIT_INDEX()` stays. Structure at the connector requires an SMT that parses the delimiter into a Struct before serialization — which is custom Java, since no stock transform splits a delimited string into fields.

Given that, structuring in Flink and letting `CREATE TABLE` register the output schema is the cheaper and cleaner route, and it is the one this project takes. The Avro schema below and [mqtt-source-avro-connector.json](./connectors/mqtt-source-avro-connector.json) remain in the repository as the device-side alternative, for the case where edge devices can legitimately hold registry credentials.

```json
{
  "type": "record",
  "name": "PartEvent",
  "namespace": "com.manufacturing.iot",
  "fields": [
    { "name": "device_id", "type": "string" },
    { "name": "line", "type": "string" },
    { "name": "parts", "type": "int" },
    { "name": "ts", "type": "string" }
  ]
}
```

### B. Windowed Reconciliation in Flink SQL
To align asynchronous telemetry with enterprise system records, Flink SQL performs a dual-stream tumbling-window aggregation and full discrepancy join. Both sides window on `$rowtime`, the Kafka record timestamp, under Confluent Cloud's default watermark strategy — see §4C for why that choice shapes the outage behaviour:

```sql
-- From flink_sql/02_windowed_reconciliation.sql (classification CASE abbreviated)
INSERT INTO count_mismatches
SELECT
    d.window_start, d.window_end, d.device_id,
    d.device_total, s.system_total,
    (d.device_total - s.system_total) AS discrepancy,
    CASE ... END AS issue_type,   -- NONE / BURST_RECOVERY / POSITIVE_BOUNCE / OUTAGE_DROP / NEGATIVE_MISSED
    CURRENT_TIMESTAMP AS detected_at
FROM (
    SELECT device_id, line, window_start, window_end, SUM(parts) AS device_total
    FROM TABLE(TUMBLE(TABLE v_device_counts, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY device_id, line, window_start, window_end
) d
JOIN (
    SELECT device_id, line, window_start, window_end, SUM(parts) AS system_total
    FROM TABLE(TUMBLE(TABLE v_system_counts, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY device_id, line, window_start, window_end
) s
  ON d.device_id = s.device_id AND d.line = s.line
 AND d.window_start = s.window_start AND d.window_end = s.window_end;
```

Two properties of this query matter more than they look. It is an **inner** join, so a
window where one side has no records — a device that is offline — produces no row at
all; the analytics views in `04` and `06` use a `FULL OUTER JOIN` and do surface it. And
it classifies on the **size** of the difference alone, which mislabels sustained
per-pulse faults; `06` adds ingestion lag as a second signal. The documentation site's
*Fault Scenarios* page works through both against the timetable below.

### C. Outage Simulation — and the finding that corrected my mental model

A simulation suite stress-tests edge network drops: `pi-03` goes offline for 70 seconds, buffers its pulses, and flushes on reconnect.

I expected the replayed events to arrive past the watermark and be discarded, producing a reconciliation deficit. **They were not discarded, and the result was a surplus in the recovery window.**

The cause is the choice of time attribute. Every window here is keyed on `$rowtime`, which Confluent documents as *"exactly the Kafka record timestamp"*. With no timestamp SMT on the source connector, that stamp is applied when the Connect worker publishes the record — so a buffered replay is published *now*, carrying a stale reading but a current timestamp. Such records are never late at any watermark tolerance; they simply aggregate into the wrong minute. The pipeline classifies this as `BURST_RECOVERY`.

**Watermark configuration, stated plainly.** These tables run Confluent Cloud's default strategy on `$rowtime` — per Kafka partition, fixed out-of-orderness tolerance of **180 milliseconds**. No custom watermark is declared in `flink_sql/`; Confluent exposes watermarks at table level (`ALTER TABLE ... MODIFY WATERMARK FOR $rowtime AS ...`), not on views.

**To obtain true event-time semantics**, the payload's `ts` field would be parsed into a `TIMESTAMP(3)`, declared as the time attribute with a watermark sized to the worst tolerable outage, and windowed on instead of `$rowtime` — or a `TimestampConverter` SMT would push the device clock into the Kafka record timestamp, leaving the SQL unchanged. The `avg_ingest_lag_sec` views already measure the gap between the two clocks, which is the input for sizing that watermark.

The tradeoff is real either way: ingestion time is monotonic, cheap, and immune to edge clock drift, but misattributes delayed work; event time attributes correctly, at the cost of held state and trust in device clocks.

---

## 5. Visualizations & Key Operational Insights from Data

The reconciliation output is surfaced in a **Streamlit** operational dashboard designed
for plant managers and QA engineers, deployed on Streamlit Community Cloud:

**[View the dashboard](https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/)**

The hosted dashboard does not hold an open Kafka consumer — running a Confluent Cloud
cluster and a local Connect worker continuously is not viable for a public demo. It
renders the reconciliation output the Flink views produce, reproduced from the
deterministic fault timetable below. Point it at a running local stack and it shows that
cluster's own output.

### Automated Hourly Glitch Schedule (Ground Truth vs. Detected Discrepancies)

To enable reproducible benchmarking and dashboard verification, the simulator runs an automated **hourly fault timetable**. Because the faults are scheduled, the ground truth is known in advance and the dashboard can be checked against it:

| Minute Window (Every Hour) | Target Machine & Line | Injected Fault Scenario | Real-World Root Cause | Dashboard Indicator |
| :--- | :--- | :--- | :--- | :--- |
| **MM:05 - MM:07** | `pi-02` (Line 1) | **Double-Bounce** (`parts: 2-3`) | Mechanical switch vibration / glare | Positive discrepancy, ~+45 per minute |
| **MM:15 - MM:17** | `pi-06` (Line 2) | **Missed Pulse** (`parts: 0`) | Optical sensor lens fogging / slip | Negative discrepancy, −30 per minute |
| **MM:25 - MM:26** | `pi-09` (Line 3) | **Clock Jitter** (`±25s` on `ts`) | PLC NTP desynchronization | **None in the pipeline** — windows run on `$rowtime`, so a shifted `ts` moves no counts. Visible only per record, as lag |
| **MM:35 - MM:36** | `pi-03` (Line 1) | **Mini Outage + Burst Replay** | Intermittent plant Wi-Fi drop | Empty windows at :35–:36 (outer-join views only), then a +60 `BURST_RECOVERY` surplus at :37 |
| **MM:48 - MM:50** | `pi-07` (Line 2) | **Overheat Double-Count** (`parts: 2`) | Overheated sensor relay chatter | Sustained positive discrepancy, +30 per minute |

> [!TIP]
> **Deactivating Glitches (Clean Baseline Mode):**
> Run `python simulators/dual_simulator.py --clean` to deactivate all glitches, timetable faults, and MES anomalies for a 100% clean baseline (0 discrepancies).
> Alternatively, pass `--no-schedule` to disable only the scheduled hourly timetable while keeping custom random glitches.

---

### Common & Important Questions Answered by the Data

#### 1. "Which assembly lines or machines have the highest failure/miscount frequency?"
- **Query / Chart:** Stacked Bar Chart of `discrepancy` counts grouped by `device_id` and `line`.
- **Operational Value:** Isolates mechanical sensor degradation (e.g., optical sensor lens fogging on `pi-06` at MM:15) before it impacts physical inventory.

#### 2. "Are discrepancies caused by under-counting (dropped pulses) or over-counting (sensor bounce)?"
- **Query / Metric:** Ratio of positive vs. negative variance:
  ```sql
  SELECT 
      device_id,
      COUNT(CASE WHEN discrepancy > 0 THEN 1 END) AS edge_overcounts,
      COUNT(CASE WHEN discrepancy < 0 THEN 1 END) AS dropped_pulses,
      AVG(ABS(discrepancy)) AS avg_parts_drift
  FROM count_mismatches
  GROUP BY device_id;
  ```
- **Operational Value:** Distinguishes network packet dropouts (negative variance) from double-pulse electrical bounce on line machinery (positive variance).

#### 3. "What is the plant-wide real-time discrepancy rate across time windows?"
- **Query / Chart:** Time-series line chart tracking aggregate hourly `discrepancy` against the total production run.
- **Operational Value:** Establishes shift-by-shift baseline reliability metrics and flags anomalous bursts during shift handover windows.

---

## 6. Results & Operational Impact

| Metric | Legacy Architecture (Batch/NiFi) | This Cloud Streaming Solution | Improvement |
| :--- | :--- | :--- | :--- |
| **Discrepancy Detection Latency** | Near real time, in NiFi flows on HDP | **Per minute** (1-min tumbling window, emitted when the window closes) | Same cadence, expressed as a few lines of declarative SQL instead of flow logic |
| **Edge Payload** | Verbose per-record envelope | **Compact `::`-delimited string, unchanged from the MiNiFi/NiFi original** | Deliberately held constant so the two implementations compare on architecture rather than serialization. No bandwidth figure is claimed — the Avro comparison was not run end to end |
| **Schema Enforcement** | Ad-hoc runtime application logic, applied per consumer | **Registered Schema Registry subject on the reconciliation output, created by `CREATE TABLE`** | Downstream consumers bind to a typed contract instead of re-parsing raw payloads; the raw hop stays deliberately schemaless |
| **Runtime Footprint** | Dedicated NiFi & Spark cluster maintenance | **Serverless Confluent Flink + a single Connect container** | Two long-running containers replaced by one, with stateful compute offloaded to Confluent Cloud |

---

## 7. Key Learnings & Engineering Takeaways

1. **Schema Registry is a producer-side contract, not a parser — and the decision is *where* to enforce it, not *whether*.**
   Registering a subject against a topic whose producer writes raw bytes does nothing: no magic byte, no schema ID, nothing to deserialize against. I registered one and watched it stay inert, which is how I learned the distinction properly. The real question is where the contract belongs. Pushing Avro to the devices means registry credentials on the plant floor; enforcing at the Flink boundary keeps the edge dumb and still gives downstream consumers a typed, registered schema. The price is parsing logic living in SQL — `MAKE_VALID_UTF8()` and `SPLIT_INDEX()` repeated across six files, with no type safety on the raw hop. That is a trade I would make again at the edge, and would not make in the middle of a platform.

2. **Know which clock your window runs on — this was the real lesson.**
   I assumed I was doing event-time processing because the column was called `event_time`. It was aliased from `$rowtime`, the Kafka record timestamp, applied at publish. The device's own clock sat unused in the payload as a string. Under that arrangement a 70-second outage produces a surplus in the recovery window, not dropped late data, and no watermark tolerance changes it. Naming a column `event_time` does not make it one: the time attribute has to be declared, and the choice between ingestion time and event time is a real architectural decision with a real tradeoff, not a formality.

3. **Hybrid Edge-to-Cloud Boundary:**
   Using a self-managed Kafka Connect container with SASL_SSL provides fine-grained network control at the plant edge while offloading stateful stream computation to Confluent Cloud.

---

- [x] **Architecture Diagrams (HLD & LLD):** [architecture.drawio](./architecture.drawio)
  - **Page 1 (HLD):** End-to-End Enterprise Flow (Edge Machinery -> Mosquitto -> Kafka Connect -> Confluent Cloud Flink -> Streamlit)
  - **Page 2 (LLD):** Internal Component Deep Dive (MQTT Delimited Payload -> ByteArrayConverter -> Flink Dual Tumbling Join on `$rowtime` -> Analytics Views; the AvroConverter + Schema Registry path is drawn as the alternative it is)
- [x] **Technical Documentation:** [README.md](./README.md) & [LEARNING.md](./LEARNING.md)
- [x] **Source Code & Flink Queries:** [flink_sql/](./flink_sql/) & [simulators/](./simulators/)
- [x] **Live Dashboard:** [iot-parts-reconciliation.streamlit.app](https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/)

