# Confluent Cloud, Schema Registry & Flink SQL: Complete Learning Guide

## 1. The Core Mystery: "Why is it `VARBINARY`? I didn't create binary fields!"

If you wrote Python code sending JSON strings like `{"device_id": "pi-01", "parts": 1}`, why did Confluent Flink give you:

```sql
CREATE TABLE `device_counts` (
  `key` VARBINARY,
  `val` VARBINARY
)
```

### The Fundamental Rule of Apache Kafka
**Kafka itself has no concept of data types, columns, or JSON.** 
To Kafka, every message is simply **two chunks of raw bytes (`byte[]`)**:
1. Key Bytes
2. Value Bytes

```
[ Your Python Script ] 
       │  (JSON String: '{"device_id":"pi-01"}')
       ▼
[ Kafka Producer ] 
       │  (Serializes string to raw bytes: [123, 34, 100, 101, ...])
       ▼
[ Kafka Topic ] ─── Stores pure binary bytes ───▶ [ 01101001 01101111 ... ]
```

---

## 2. What Happens When Confluent Cloud Flink Sees a Topic

When you open Confluent Cloud Flink and look at a Kafka topic, Flink has to decide: *"What columns and data types should this table have?"*

There are two completely different worlds in Confluent:

```
                               How Flink Sees Your Topic
                                          │
            ┌─────────────────────────────┴─────────────────────────────┐
            ▼                                                           ▼
 [ World A: Schemaless Topic ]                                [ World B: Schema-Managed Topic ]
 (Raw JSON / Plain text)                                      (Avro / Protobuf / JSON Schema via SR)
            │                                                           │
   Flink does NOT know the                                    Schema Registry tells Flink
   internal fields beforehand.                                exact column names & types!
            │                                                           │
 Flink creates a fallback table:                              Flink automatically creates:
   `key` VARBINARY (raw bytes)                                  `device_id` STRING
   `val` VARBINARY (raw bytes)                                  `line`      STRING
                                                                `parts`     INT
                                                                `ts`        STRING
```

### Why Flink defaults to `VARBINARY` for Schemaless topics:
Because anyone can send any bytes into Kafka, Flink cannot guess your JSON structure safely. To avoid breaking or dropping bytes, Flink assigns the generic binary type **`VARBINARY`** to `key` and `val`.

That is why you had to cast the bytes to a string before parsing:
```sql
-- Convert raw binary bytes -> UTF-8 String -> Parse JSON property
CAST(JSON_VALUE(CAST(`val` AS STRING), '$.device_id') AS STRING) AS device_id
```

---

## 3. How Confluent Schema Registry Solves This

In production streaming architectures, you **never parse raw JSON strings with `JSON_VALUE` in SQL** because:
1. It is slow (parsing JSON on every streaming record burns CPU).
2. If a developer accidentally typos a field name (e.g. `deviceId` instead of `device_id`), downstream jobs silently receive `NULL` and break.

This is where **Confluent Schema Registry** comes in.

### How Schema Registry Works:

1. **Schema Definition (`part_event.avsc`):** You declare the contract up front:
   ```json
   {
     "type": "record",
     "name": "PartEvent",
     "fields": [
       {"name": "device_id", "type": "string"},
       {"name": "line", "type": "string"},
       {"name": "parts", "type": "int"},
       {"name": "ts", "type": "string"}
     ]
   }
   ```

2. **Write-time Registration:**
   When your Python script or Kafka Connect produces with Avro:
   - The producer registers the schema with Schema Registry **once** (getting e.g. `Schema ID: 42`).
   - The producer prepends just 5 bytes to each message: `[Magic Byte (0)] + [Schema ID 42 (4 bytes)] + [Compact Binary Avro Payload]`.

3. **Flink Integration (Zero SQL boilerplate):**
   - Confluent Flink reads `Schema ID 42` from Schema Registry.
   - Flink **automatically creates the table with native columns** (`device_id`, `line`, `parts`, `ts`).
   - You can immediately write clean SQL without any casts:
     ```sql
     SELECT device_id, SUM(parts) FROM device_counts GROUP BY device_id;
     ```

---

## 4. Comparing the Approaches

| Feature | Raw JSON (What you used initially) | Avro + Schema Registry (Enterprise Standard) |
|---|---|---|
| **Topic Storage** | Plain text JSON string | Compact binary Avro (3-5x smaller payload) |
| **Flink Table Structure** | `key VARBINARY`, `val VARBINARY` | `device_id STRING`, `parts INT`, etc. automatically |
| **SQL Query Complexity** | Requires `JSON_VALUE(CAST(val AS STRING), '$.field')` | Direct column names: `SELECT device_id, parts` |
| **Schema Evolution** | No safety (breaking changes go unnoticed) | Enforced compatibility rules (`BACKWARD`, `FULL`) |
| **Performance** | High CPU overhead (JSON string parsing) | Fast binary deserialization |

---

## 5. Kafka Partitioning: How Scaling & Ordering Work

A Kafka Topic is not a single queue; it is split into **Partitions** (like lanes on a highway).

```
Topic: device_counts (3 Partitions)

Partition 0: [msg 0] [msg 1] [msg 2] [msg 3] ──▶ Offset = 4
Partition 1: [msg 0] [msg 1] [msg 2]         ──▶ Offset = 3
Partition 2: [msg 0] [msg 1] [msg 2] [msg 3] ──▶ Offset = 4
```

### The Cardinal Rule of Kafka Partitioning:
> **Kafka ONLY guarantees message ordering WITHIN a single partition, never across partitions.**

### How Kafka decides which partition a message goes to:
1. **With a Key (e.g. `key = "pi-01"`):**
   - Kafka computes `hash(key) % num_partitions`.
   - All events for `"pi-01"` **always** land in the exact same partition (e.g., Partition 1).
   - **Why this matters for your factory:** Machine `pi-01` produces parts sequentially over time (tick 1, tick 2, tick 3). Because they are in the same partition, Flink processes them in strict chronological order.
2. **Without a Key (`key = NULL`):**
   - Kafka uses sticky round-robin. Messages are distributed randomly across all partitions.
   - Per-device ordering is lost.

---

## 6. What are Offsets & `connect-offsets`?

### What is an Offset?
An **Offset** is a sequential, incrementing integer ID assigned to every message committed to a partition (e.g. Offset 0, 1, 2, 3...).
It acts like a **bookmark** in a book.

### Standard Consumer Offsets (`__consumer_offsets`)
When Flink or a Python consumer reads from Kafka, it commits its current bookmark into Kafka's internal `__consumer_offsets` topic:
```
"Consumer Group A has read up to Partition 0, Offset 1042"
```
If your Flink job or consumer crashes and restarts, it checks `__consumer_offsets` and resumes immediately from offset `1043` without reprocessing historical data or dropping new data.

---

## 7. Kafka Connect Internal Topics (`_iot-connect-offsets`)

Kafka Connect runs as a distributed cluster of worker tasks. It uses 3 internal compacted Kafka topics to maintain its state:

| Internal Topic | Purpose | Example Stored Data |
|---|---|---|
| **`_iot-connect-offsets`** | Stores source & sink connector progress | Source: *"Last MQTT message timestamp read was 09:45:00Z"*<br>Sink: *"Last Kafka offset written to Postgres was 4500"* |
| **`_iot-connect-configs`** | Stores connector JSON definitions | The active config for `mqtt-source-device-counts` |
| **`_iot-connect-status`** | Stores running task health & status | `RUNNING`, `PAUSED`, `FAILED` |

### How Kafka Connect Recovers From Crashes (Resilience):
```
1. Connect Worker is streaming records from Mosquitto to Kafka.
2. Every few seconds, it commits its source progress to `_iot-connect-offsets`.
3. Worker container crashes (or is killed with `docker restart`).
4. On reboot, Connect Worker reads `_iot-connect-offsets`.
5. It resumes reading MQTT exactly where it left off!
```

---

## 8. Summary Mental Model

- **Partition** = A parallel lane in Kafka. All events for the same `device_id` go to the same lane so they stay in strict chronological order.
- **Offset** = The bookmark number showing how far a consumer has read.
- **`connect-offsets`** = The persistent notebook Kafka Connect uses to remember what it has already copied over from MQTT and what it has written to Postgres.

---

## 9. Comprehensive Interview Preparation Guide

This section equips you with Tier 1 talking tracks, deep architectural rationales, and exact answers to questions senior data and streaming engineering interviewers ask about this project.

---

### Q1: "Why did you use MQTT at the edge instead of producing directly to Kafka from the Raspberry Pi devices?"

#### Deep Dive & Rationale:
At the physical edge, factory sensors and microcontrollers (Raspberry Pis / PLCs) run in hostile network environments (factory RF interference, Wi-Fi handoff jitter, or cellular uplinks). 

1. **Connection & Protocol Overhead:**
   - A Kafka producer maintains persistent TCP connections to every partition leader across multiple brokers, requiring heartbeats, metadata refreshes, and TLS/SASL handshakes. This is too heavy for constrained edge microcontrollers.
   - MQTT (Message Queuing Telemetry Transport) is a minimal, binary protocol designed specifically for low-bandwidth, battery-constrained devices with a 2-byte header footprint.
2. **Security & Topology:**
   - Opening outbound cloud connections from 50+ individual edge devices on a plant floor violates plant network security policies (ISA-95 / Purdue model).
   - Terminating MQTT traffic locally at an on-premise Mosquitto broker keeps edge traffic internal. A single, managed **Kafka Connect worker** acts as the secure, authenticated outbound TLS tunnel to Confluent Cloud.

> **Your Answer:**
> *"At the physical edge, 50+ Raspberry Pis and PLCs operate over factory Wi-Fi and intermittent cellular networks with constrained memory. Running a full Java Kafka client or opening persistent SSL/TLS TCP producer sessions from dozens of edge devices creates heavy connection overhead and fails during transient network dips.*
> 
> *MQTT with QoS 1 is a lightweight pub/sub protocol specifically built for edge battery/RAM constraints. We terminate the MQTT traffic locally at an on-premise Mosquitto broker, and use a self-managed distributed **Kafka Connect worker** as the secure, high-throughput bridge into Confluent Cloud."*

---

### Q2: "How do you guarantee event ordering in Kafka when multiple devices publish concurrently?"

#### Deep Dive & Rationale:
- A common misconception is that Kafka topics are globally ordered. **Kafka only guarantees ordering within a single partition.**
- If events from `pi-01` are written across different partitions round-robin, a consumer reading partition 1 might process Event #2 before another consumer on partition 0 processes Event #1.
- By configuring Kafka Connect with the `ValueToKey` Single Message Transform (SMT) targeting `device_id`, Kafka computes `MurmurHash2(device_id) % num_partitions`. 
- Every event generated by `pi-01` is mathematically guaranteed to land in the exact same partition in sequential offset order.

> **Your Answer:**
> *"Kafka only guarantees ordering **within a single partition**, not across the entire topic. 
> To maintain strict per-device chronological order, we explicitly set the Kafka record key to `device_id` (via Single Message Transform `ValueToKey` in Connect). 
> Kafka hashes the `device_id` so that all events for a specific machine (e.g. `pi-01`) always land in the same partition in sequential offset order, allowing Flink to process each machine's event timeline accurately."*

---

### Q3: "How does Kafka Connect recover if a worker node crashes mid-stream? Does it lose data or duplicate records?"

#### Deep Dive & Rationale:
In distributed mode, Kafka Connect is entirely stateless on disk. It maintains state in 3 internal compacted Kafka topics:
- `_iot-connect-offsets`: Stores source connector offsets (e.g. MQTT timestamp/message ID) and sink offsets.
- `_iot-connect-configs`: Stores connector definitions.
- `_iot-connect-status`: Stores task assignments.

When a worker crashes:
1. The Kafka Connect group coordinator triggers a **rebalance**.
2. Another active worker (or the restarted worker) is assigned the task.
3. The worker reads `_iot-connect-offsets` from Kafka and resumes consumption from the last committed offset.
4. Combined with idempotent writes or primary key deduplication in the sink database, this guarantees **at-least-once to effectively-once delivery**.

> **Your Answer:**
> *"In distributed mode, Kafka Connect does not store state on local disk. It uses internal, compacted Kafka topics hosted on the cluster: `connect-offsets`, `connect-configs`, and `connect-status`.*
> 
> *During streaming, the MQTT Source connector periodically flushes source offset bookmarks to `connect-offsets`. When a worker node crashes and recovers, the reassigned task reads `connect-offsets` and resumes consumption from the exact point of the last committed offset."*

---

### Q4: "Why use Flink SQL instead of micro-batching (Spark Streaming) or querying a relational database for reconciliation?"

#### Deep Dive & Rationale:
- **Relational DB Polling:** Querying PostgreSQL every second with `GROUP BY 1-minute` causes severe table locking, high IOPS, and cannot handle millions of events per second.
- **Spark Streaming (Micro-batching):** Spark groups events into artificial 500ms–2s batches. While powerful for large ETL, micro-batching introduces artificial latency and requires separate state management for cross-batch joins.
- **Flink SQL (Continuous Event-Driven Streaming):** Flink evaluates events record-by-record. In a 1-minute tumbling window join, Flink holds state in its managed state backend (RocksDB/Memory) and emits discrepancies the instant the event-time watermark crosses the window boundary.

> **Your Answer:**
> *"Factory piece reconciliation requires continuous, low-latency evaluation of physical pulse counts against MES records. 
> Doing micro-batching or querying a relational database introduces polling latency and high query load. 
> 
> With Flink SQL, we run stateful **1-minute tumbling window stream-to-stream joins**. Flink maintains in-memory state for the active 1-minute window, and as soon as the event-time watermark passes the window boundary, it emits discrepancies in real-time without slamming downstream databases."*

---

### Q5: "What happens when a factory machine loses power for 70 seconds and bursts late data? How did you handle that in Flink?"

#### Deep Dive & Rationale (Your Core Engineering Story):
This is the bridge between real-world plant operations and streaming theory:
1. Machine `pi-03` drops offline during a power dip, but buffers produced parts in its local RAM buffer.
2. The remaining 9 machines continue streaming, causing Flink's event-time clock and watermark to advance past the 1-minute window.
3. When `pi-03` reconnects and blasts its 70-second backlog, Flink's watermark has already closed that window.
4. **Outcome:** Flink drops the late events, triggering a discrepancy alert (`device_total < system_total`).
5. **The Tradeoff:**
   - *Widening the watermark (e.g. 2 minutes):* Accommodates late bursts without dropping data, but delays alert emission for the entire plant by 2 minutes and increases Flink state memory usage.
   - *Tight watermark (10 seconds):* Gives instant real-time alerting, but flags late-replaying machines as discrepancies. In discrete manufacturing, an immediate discrepancy alert is preferred so maintenance teams know a machine experienced network lag.

> **Your Answer:**
> *"This was the most interesting failure mode we modeled. When machine `pi-03` disconnected for 70 seconds and replayed its buffered events, the events arrived with event timestamps older than Flink's 10-second watermark boundary.*
> 
> *By default, Flink drops data that falls behind the watermark, which caused an immediate discrepancy alert in `count_mismatches`. 
> We evaluated the tradeoff: widening the watermark tolerance (e.g. to 60s) allows Flink to absorb late bursts, but holds state in memory for an extra minute and increases alert latency. For real-time alarming on a factory floor, we kept a tight 10-second watermark and flagged late arrivals as an alert."*

---

### Q6: "Why use Confluent Schema Registry with Avro instead of sending raw JSON?"

#### Deep Dive & Rationale:
1. **Serialization Efficiency:** Avro binary serialization is 3–5x smaller than JSON because field names are not repeated in every single payload.
2. **Performance in Flink:** Parsing raw JSON in Flink requires invoking `JSON_VALUE()` on every single row, consuming excessive CPU. With Avro, Schema Registry registers the schema directly into the Flink catalog, giving native typed columns (`STRING`, `INT`, `TIMESTAMP`).
3. **Contract Enforcement & Schema Evolution:** Schema Registry enforces `BACKWARD` or `FULL` compatibility rules at write time. If a developer attempts to remove a required field or change a type, Schema Registry rejects the write before bad data pollutes Kafka.

> **Your Answer:**
> *"Raw JSON has two big production problems:
> 1. In Flink, querying raw JSON requires parsing strings with `JSON_VALUE` on every record, which wastes CPU.
> 2. There is no schema validation at write-time—if someone renames `device_id` to `deviceId`, downstream jobs silently fail with NULLs.
> 
> With Schema Registry and Avro:
> - The schema contract (`part_event.avsc`) is enforced at the producer level with `BACKWARD` compatibility rules.
---

## 7. Edge Fault Simulation & Benchmarking Modes

The simulator supports two distinct operating modes to facilitate clear demo presentation and benchmark testing:

### Mode A: Clean Baseline Mode (`--clean`)
- **Behavior:** 0 glitches, 0 clock drifts, 0 MES noise, 0 timetable faults.
- **Expected Outcome:** 100% perfect reconciliation. `count_mismatches` table remains empty (or variance = 0).
- **Execution:**
  ```bash
  python simulators/dual_simulator.py --clean
  ```

### Mode B: Fault & Glitch Injection Mode (Default)
- **Behavior:** Injects automated hourly faults according to a fixed schedule:
  - `MM:05 - MM:08`: `pi-02` Sensor Double-Bounce (`parts: 2-3`)
  - `MM:15 - MM:18`: `pi-06` Missed Pulse (`parts: 0`)
  - `MM:25 - MM:27`: `pi-09` Clock Jitter (`±25s`)
  - `MM:35 - MM:37`: `pi-03` Mini Outage & Burst Replay
  - `MM:48 - MM:51`: `pi-07` Overheat Double-Count
- **Execution:**
  ```bash
  # Standard fault timetable + 15% MES noise
  python simulators/dual_simulator.py

  # Deactivate timetable but keep random 10% glitch rate
  python simulators/dual_simulator.py --no-schedule --glitch-rate 0.10
  ```

---

## 10. PostgreSQL `generate_series` vs Flink SQL `TUMBLE` (Window TVFs)

When transitioning from relational databases like PostgreSQL to stream processing engines like Apache Flink, one of the most common architectural shifts is how time-based aggregation windows are generated.

---

### In PostgreSQL (Batch / Static Tables):
PostgreSQL operates on bounded, stored historical datasets. To generate continuous 1-minute time buckets over a window of time, you typically synthesize time rows using **`generate_series()`** and join against your data table with `date_trunc()`:

```sql
-- PostgreSQL Approach: Synthetic interval generation
SELECT 
    b.time_bucket AS window_start,
    b.time_bucket + INTERVAL '1 minute' AS window_end,
    d.device_id,
    COALESCE(SUM(d.parts), 0) AS total_parts
FROM generate_series(
    '2026-09-30 19:00:00'::timestamptz, 
    '2026-09-30 20:00:00'::timestamptz, 
    INTERVAL '1 minute'
) AS b(time_bucket)
LEFT JOIN device_counts d 
  ON date_trunc('minute', d.event_time) = b.time_bucket
GROUP BY b.time_bucket, d.device_id;
```

---

### In Apache Flink SQL (Continuous Unbounded Streams):
Flink processes continuous, infinite event streams in real time as events arrive over the network. It uses modern SQL standard **Windowing Table-Valued Functions (TVFs)**:

```sql
-- Flink SQL Approach: Native Streaming Window TVF
SELECT
    device_id,
    line,
    window_start,
    window_end,
    SUM(parts) AS device_total
FROM TABLE(
    TUMBLE(
        TABLE v_device_counts, 
        DESCRIPTOR(event_time), 
        INTERVAL '1' MINUTE
    )
)
GROUP BY device_id, line, window_start, window_end;
```

---

### Deep Dive: Breaking Down the Flink Window Syntax

Let's dissect the exact components of `TABLE(TUMBLE(...))`:

```
                       TABLE( ... )  ─── 1. Table-Valued Function (TVF) Wrapper
                         │
                         ▼
        ┌─────────────────────────────────────────────────────────────┐
        │  TUMBLE(                                                    │
        │      TABLE v_device_counts,    ── 2. Source stream          │
        │      DESCRIPTOR(event_time),   ── 3. Time column descriptor │
        │      INTERVAL '1' MINUTE       ── 4. Fixed window duration  │
        │  )                                                          │
        └─────────────────────────────────────────────────────────────┘
```

#### 1. What is a Table-Valued Function (`TABLE(...)`)?
- In standard SQL, normal functions like `UPPER('text')` or `SUM(x)` return a **single scalar value**.
- A **Table-Valued Function (TVF)** takes a table/stream as input and **returns an entirely new virtual table** with new computed columns attached.
- The `TABLE(...)` keyword informs the SQL engine: *"Treat the output of this function as a real table that I can query with `SELECT`, `WHERE`, `GROUP BY`, or `JOIN`."*

#### 2. `TABLE v_device_counts` (Source Stream)
- Specifies the underlying stream or view whose rows should be partitioned into time windows.

#### 3. `DESCRIPTOR(event_time)` (Timestamp Column)
- `DESCRIPTOR(col)` passes the **column identifier metadata** itself rather than evaluating its scalar value on each row.
- This tells Flink to align windows to the event's embedded **watermarked event-time** (`$rowtime`), ensuring late or out-of-order IoT data lands in the correct time slice.

#### 4. `INTERVAL '1' MINUTE` (Window Size)
- Defines non-overlapping, contiguous time buckets (e.g., `19:00:00–19:01:00`, `19:01:00–19:02:00`).

#### 5. Output Columns Generated by `TUMBLE`:
`TABLE(TUMBLE(...))` passes through all original columns and **automatically injects 3 new window columns**:
- `window_start`: Window starting timestamp (inclusive).
- `window_end`: Window ending timestamp (exclusive).
- `window_time`: The window watermark timestamp.

---

### Key Differences & Architectural Advantages:

| Feature | PostgreSQL `generate_series()` | Flink SQL `TUMBLE(...)` TVF |
|---|---|---|
| **Data Nature** | Bounded / Static historical tables | Unbounded / Infinite real-time stream |
| **Window Boundary Trigger** | Static query execution time | **Watermark-driven** (event-time completion) |
| **Out-of-Order Handling** | Must re-scan entire table | Handled via watermark lateness tolerance |
| **State Retention** | Reads from disk tables on demand | Managed in Flink state backend (RocksDB/Memory) |
| **Output Emission** | Single batch result on query completion | **Emits continuously** per window boundary close |

---

## 11. Edge Clock Jitter & Window Spillover Mechanics

### What is Clock Jitter?
Clock jitter (or clock drift) occurs when an edge IoT sensor / PLC microcontroller loses synchronization with the plant's true time (or Cloud MES server clock) due to missing NTP servers or RTC hardware drift.

### How Clock Jitter Breaks Stream Windowing (The Spillover Effect):
When a machine produces parts steadily at 1 part/sec, but its edge clock drifts backwards by e.g. 20 seconds, parts produced in Minute 2 get event-time stamped with Minute 1 timestamps.

```
                   1-Minute Window A (18:59 - 19:00)         1-Minute Window B (19:00 - 19:01)
                ┌──────────────────────────────────────┐  ┌──────────────────────────────────────┐
MES Record      │ Count: 30 parts                      │  │ Count: 30 parts                      │
                │                                      │  │                                      │
Edge Device     │ Count: 31 parts (+1 extra spilled in)│  │ Count: 29 parts (-1 missing part)    │
                └──────────────────────────────────────┘  └──────────────────────────────────────┘
                                  │                                         │
                                  ▼                                         ▼
                        Discrepancy: +1                           Discrepancy: -1
```

### The Signature of Clock Jitter:
1. **Window A:** $+N$ positive discrepancy.
2. **Immediate Window B:** $-N$ negative discrepancy.
3. **Net Total:** **Zero net error** across the full window interval $(+N - N = 0)$. No parts were physically lost or gained; timestamps simply drifted across the tumbling window boundary.

---

## 12. Watermark Tolerance Experiments & Session Windows

File reference: [03_watermark_experiments.sql](file:///c:/Users/saqib.tamli/Documents/Repos/git_saqie/saqib-resume/projects/iot-parts-reconciliation/flink_sql/03_watermark_experiments.sql)

In real-world distributed streaming, network jitter and factory power dips mean events do not arrive in perfect chronological order. Watermarks are the mechanism Flink uses to declare: *"We believe all events prior to time $T$ have arrived; we can now safely close the window and emit results."*

---

### Scenario A: Strict Watermark (`event_time - INTERVAL '5' SECOND`)

```sql
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
```

```
Event Stream: ──[19:00:10]───[19:00:20]───[19:00:30]───[19:01:05]
                                                               │
                                         Watermark advances to: 19:01:00 (19:01:05 - 5s)
                                         Window [19:00 - 19:01] CLOSES & EMITS INSTANTLY!
```

- **Pros:**
  - **Ultra-low latency:** Discrepancy alerts emit within 5 seconds of the physical minute ending.
  - **Minimal memory/state footprint:** Flink flushes window aggregations from state immediately.
- **Cons:**
  - **Drops late data:** If a machine loses Wi-Fi for 15 seconds and sends buffered events with timestamps older than 5s behind the watermark, Flink silently drops them or flags them as missing parts.

---

### Scenario B: Relaxed Watermark (`event_time - INTERVAL '60' SECOND`)

```sql
WATERMARK FOR event_time AS event_time - INTERVAL '60' SECOND
```

```
Event Stream: ──[19:00:10]───[19:00:20]───[19:01:05]───[19:02:00]
                                                               │
                                         Watermark advances to: 19:01:00 (19:02:00 - 60s)
                                         Window [19:00 - 19:01] CLOSES HERE (1 min later)
```

- **Pros:**
  - **High fault tolerance:** Absorbs edge machine mini-outages (e.g. `pi-03` rebooting and bursting 45 seconds of buffered data) without dropping events.
- **Cons:**
  - **High alert latency:** Downstream alerts for the `19:00 - 19:01` window are delayed until `19:02:00` (an extra 60s delay).
  - **Higher RAM consumption:** Flink must hold RocksDB/memory state for all 50+ machines open for an extra 60 seconds.

---

### Summary Comparison: The Watermark Tradeoff Curve

| Property | Strict Watermark (5s) | Relaxed Watermark (60s) |
|---|---|---|
| **Alert Latency** | Near real-time (~5s after minute ends) | Delayed (~60s after minute ends) |
| **State Memory Size** | Very Small (flushed immediately) | Larger (held in memory buffer for 60s) |
| **Tolerance to Network Jitter** | Fragile (drops bursts >5s late) | High (absorbs bursts up to 60s late) |
| **Best Used For** | Urgent safety stops, live visual dashboards | Accurate financial reconciliation, audit logs |

---

### Scenario C: Session Windows for Inactivity Detection (Dead Machine / Silent Failure)

```sql
SELECT
    device_id,
    SESSION_START(event_time, INTERVAL '3' MINUTE) AS session_start,
    SESSION_END(event_time, INTERVAL '3' MINUTE) AS session_end,
    COUNT(*) AS event_count
FROM device_counts
GROUP BY
    device_id,
    SESSION(event_time, INTERVAL '3' MINUTE);
```

#### How Session Windows Work:
Unlike `TUMBLE` (fixed 1-minute blocks), a **`SESSION` Window** has no fixed duration. It groups events into an active "session" as long as new events keep arriving within the **gap threshold (3 minutes)**.

```
Device Events: ──[●]──[●]──[●]────────────────────────[●]──[●]───▶
                 └── Active Session 1 ──┘  Gap > 3 min  └── Session 2 ──┘
```

- When a device stops producing pulses for longer than 3 minutes, the session **closes and emits**.
- **Factory Floor Value:** Instantly flags **silent machine failures** (e.g., disconnected sensor cable, PLC firmware freeze) without needing separate heartbeat infrastructure.

---

## 9. Event Time vs Ingestion Time: The Dual-Clock Reality

In distributed stream processing, every event has two timestamps:

```
[ 1. Event Time (ts) ]       ───► The physical moment the part was stamped on the factory floor.
[ 2. Ingestion Time ($rowtime) ] ──► The moment Confluent Kafka / Flink received the network packet.
```

### Why Measuring Edge-to-Cloud Lag Matters
```sql
SELECT
    device_id,
    line,
    ts AS edge_device_ts,
    event_time AS flink_ingest_ts,
    TIMESTAMPDIFF(
        SECOND, 
        TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), 
        event_time
    ) AS edge_to_cloud_lag_seconds,
    parts,
    status
FROM v_device_counts
WHERE device_id IN ('pi-03', 'pi-09');
```

- **Normal Steady-State:** Lag is `0s` to `2s`.
- **Clock Jitter (`pi-09` at MM:25):** Lag jumps to `-25s` or `+25s` because the Raspberry Pi OS clock drifted.
- **Outage Reconnection Burst (`pi-03` at MM:35-37):** Lag jumps to `70s - 120s` as buffered memory pulses are flushed to the cloud.

### Why Ingestion Time Corrupts Count Windows
If you window by Ingestion Time instead of Event Time:
1. During an outage, counts drop to `0` (false downtime alarm).
2. When the machine reconnects, 90 buffered parts arrive in 1 second (false overproduction alarm).
3. **By using Event Time + Watermarks**, Flink properly sorts all 90 parts back into their original 1-minute time boxes!

---

## 10. Real-Time Stream Enrichment (Dimension & Lookup Joins)

Raw IoT events are lightweight: `(device_id, line, parts, ts)`. 
To turn raw data into operational intelligence, Flink enriches the stream with business metadata in real time:

```sql
SELECT 
    d.event_time,
    d.device_id,
    d.line,
    d.parts,
    CASE 
        WHEN d.device_id IN ('pi-01', 'pi-02', 'pi-03') THEN 'Stamping & Press Zone A'
        WHEN d.device_id IN ('pi-04', 'pi-05', 'pi-06') THEN 'Robotic Welding Cell B'
        ELSE 'Final Assembly & Inspection'
    END AS plant_zone,
    CASE 
        WHEN d.device_id IN ('pi-02', 'pi-07') THEN 'Optical Sensor (Bounce Prone)'
        WHEN d.device_id = 'pi-06' THEN 'Inductive Proximity (Miss Prone)'
        WHEN d.device_id = 'pi-03' THEN 'Edge Gateway (Outage Prone)'
        ELSE 'Standard Relay'
    END AS sensor_tech
FROM v_device_counts d;
```

**Key Streaming Principle:** Telemetry events stay lean (saving edge bandwidth), while Flink handles contextual enrichment in the cloud before pushing alerts downstream.

---

## 11. Complex Event Processing (Flink CEP via `MATCH_RECOGNIZE`)

Standard SQL aggregations (`SUM`, `COUNT`) calculate values over fixed time boxes.
**Flink CEP** detects **temporal sequences and state machine transitions** across multiple rows.

### Pattern 1: Rapid Optical Bounce Flapping (3+ Glitches in 20s)
```sql
SELECT *
FROM v_device_counts
    MATCH_RECOGNIZE (
        PARTITION BY device_id
        ORDER BY event_time
        MEASURES
            FIRST(A.event_time) AS pattern_start,
            LAST(B.event_time) AS pattern_end,
            COUNT(B.parts) + 1 AS consecutive_bounces,
            SUM(B.parts) + FIRST(A.parts) AS total_parts_produced
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A B+) WITHIN INTERVAL '20' SECOND
        DEFINE
            A AS A.parts > 1,
            B AS B.parts > 1
    );
```

### Pattern 2: Consecutive Zero-Pulse Sensor Blindness
Detects when a proximity sensor is physically blocked and produces 2 consecutive `parts = 0` pulses within 15 seconds:
```sql
SELECT *
FROM v_device_counts
    MATCH_RECOGNIZE (
        PARTITION BY device_id
        ORDER BY event_time
        MEASURES
            FIRST(A.event_time) AS failure_start,
            LAST(B.event_time) AS failure_end,
            'CRITICAL: CONSECUTIVE ZERO COUNTS DETECTED' AS alert_msg
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A B) WITHIN INTERVAL '15' SECOND
        DEFINE
            A AS A.parts = 0,
            B AS B.parts = 0
    );
```

### Pattern 3: Outage Recovery Spike / Rapid Burst Detector
Detects when a machine reconnects and emits $> 10$ pulses within 5 seconds:
```sql
SELECT *
FROM v_device_counts
    MATCH_RECOGNIZE (
        PARTITION BY device_id
        ORDER BY event_time
        MEASURES
            FIRST(BURST.event_time) AS burst_start,
            LAST(BURST.event_time) AS burst_end,
            COUNT(BURST.parts) AS total_burst_events,
            SUM(BURST.parts) AS total_burst_parts,
            'RECOVERY FLUSH DETECTED' AS incident_tag
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (BURST{10,}) WITHIN INTERVAL '5' SECOND
        DEFINE
            BURST AS TRUE
    );
```

---

## 12. Real-Time Analytics & Streamlit Dashboard Architecture

Traditional BI tools (Tableau, PowerBI) are pull-based and struggle with continuous push streams. By extracting structured discrepancy records from Flink SQL into a lightweight Streamlit application, we achieve zero-cost, real-time analytics.

### Streamlit Dashboard Highlights (`dashboard.py`):
1. **Executive Fleet KPIs:** Total edge pulse count, MES plan target, net discrepancy ($\Delta$), and fleet reconciliation accuracy %.
2. **1-Minute Window Drift Chart:** Interactive Plotly time-series comparing physical vs. enterprise production with discrepancy bars.
3. **Root-Cause Anomaly Taxonomy:** Interactive donut chart categorizing `POSITIVE_BOUNCE`, `NEGATIVE_MISSED`, `CLOCK_JITTER`, and `BURST_RECOVERY`.
4. **Edge-to-Cloud Lag Gauge:** Live monitoring of $(rowtime - ts)$ buffer flush latency across production lines.
5. **Incident Feed:** Real-time filterable table for manufacturing operations teams.

