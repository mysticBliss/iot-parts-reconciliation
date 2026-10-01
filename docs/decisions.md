---
description: "Architecture decision records for the pipeline: what was chosen, the alternatives considered, and what each choice cost."
---

# Decisions

Each decision as a short record: what was chosen, what else was on the table, and what
it cost. The costs are the interesting part.

---

### D1 · MQTT at the edge, not Kafka producers

**Chosen:** Devices publish MQTT QoS 1 to a local Mosquitto broker.
**Instead of:** A Kafka producer on each device; HTTP to a cloud endpoint.
**Because:** Kafka clients are heavy for small devices and need cloud credentials and
outbound connections from the plant network. MQTT is built for exactly this.
**Cost:** An extra hop and a broker to run. QoS 1 means possible duplicates, which the
pipeline doesn't remove.

---

### D2 · Self-managed Connect worker at the edge

**Chosen:** Kafka Connect in distributed mode, in Docker next to the broker.
**Instead of:** A Confluent fully managed MQTT connector; a custom bridge script.
**Because:** Managed connectors run in the cloud and can't reach a private plant broker
without peering or a tunnel. A local worker makes one outbound TLS connection.
**Cost:** Something to run, patch and monitor on site. It's also the single point
holding a cloud credential.

---

### D3 · Schemaless at the edge, schema at the Flink boundary

**Chosen:** `ByteArrayConverter`, `::`-delimited payload; typed schema registered by
`CREATE TABLE count_mismatches`.
**Instead of:** Avro with Schema Registry on the devices; a custom parsing SMT at Connect.
**Because:** Avro's wire format needs the *producer* to talk to Schema Registry — that's
registry credentials and cloud egress on every device. Raw capture can't break on a
shape change.
**Cost:** Parsing repeated in SQL, no type safety on the raw hop, nothing stopping field
reordering. → [Chapter 4](story/schema.md)

---

### D4 · Window on `$rowtime` (ingestion time)

**Chosen:** `$rowtime AS event_time`, Confluent's default 180 ms watermark.
**Instead of:** Event time parsed from `ts`; a timestamp SMT at the connector.
**Because:** Honestly — it was the default, and I didn't realise it was a choice until
the outage test. In hindsight it *is* defensible: monotonic, cheap, immune to bad device
clocks.
**Cost:** Delayed data lands in the wrong minute; clock-jitter faults are invisible.
→ [Chapter 3](story/outage.md), [Chapter 5](story/silent-faults.md)

---

### D5 · Per-device, per-minute tumbling windows

**Chosen:** One-minute `TUMBLE`, grouped by device and line, on both streams.
**Instead of:** Hopping or session windows; per-line totals.
**Because:** One minute is short enough to act within a shift and long enough to
smooth single-pulse noise. Per device, because the fault is always in one machine.
**Cost:** A fault that straddles a boundary splits across two windows. Per-device rows
mean 10 rows a minute even when nothing is wrong.

---

### D6 · Inner join in `02`, full outer join in `04` / `06`

**Chosen:** Both, in different files — the core reconciliation uses an inner join, the
enrichment and analytics views a full outer join with `COALESCE(…, 0)`.
**Because:** Not a deliberate split. The inconsistency only became visible when tracing
the outage minute by minute.
**Cost:** `count_mismatches` can't see a window where one side is empty, which is the
outage case. **This should be a full outer join everywhere.**
→ [Chapter 3](story/outage.md#where-did-the-deficit-go)

---

### D7 · Streamlit, after Postgres + Superset

**Chosen:** Flink analytics views rendered in Streamlit.
**Instead of:** JDBC sink to PostgreSQL with Superset — which was built first.
**Because:** The relational hop added a database and a second container stack without
changing what the operator sees.
**Cost:** No persisted history outside Kafka retention. A real deployment would want a
sink for audit.

---

### D8 · A replayed, not live, public dashboard

**Chosen:** The hosted app generates data from the fault timetable.
**Instead of:** A consumer on a cluster left running.
**Because:** Paying for a cluster around the clock for a demo isn't sensible.
**Cost:** The page can drift from the pipeline — and it has.
→ [Chapter 6](story/dashboard.md)
