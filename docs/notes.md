---
description: "Short answers to the questions this streaming system raises — time semantics, schemas, joins, classification and delivery guarantees."
---

# Notes to Remember

The questions this system makes you able to answer, with the short version of each.
Every one links to the page with the long version.

---

## The system

**What does it do, in one sentence?**
Every minute, it compares how many parts each machine says it made with how many the MES
recorded, and labels the difference.

**Why rebuild a 2018 project?**
Because I already understood the problem, so the only new thing was the stack. Same
payload, same faults, different architecture. → [Chapter 1](story/origin.md)

**Why MQTT and not Kafka at the edge?**
Small devices, bad networks, and no cloud credentials on the plant floor. MQTT is built
for that; Kafka clients aren't. → [D1](decisions.md#d1-mqtt-at-the-edge-not-kafka-producers)

**Why self-managed Connect?**
A managed connector runs in the cloud and can't reach a private plant broker. A local
worker makes one outbound connection instead. → [Kafka Connect](architecture/connect.md)

---

## Time

**What clock do the windows run on?**
`$rowtime` — the Kafka record timestamp, set when Connect publishes. Ingestion time, not
device time. → [Time](concepts/time.md)

**What watermark?**
Confluent's default: max timestamp per partition minus a fixed 180 ms. Nothing custom is
declared.

**What happens when a device goes offline for two minutes?**
Its windows are empty (a deficit, but only in the outer-join views), then the replay
lands in the recovery minute as a surplus. The parts aren't lost, they're in the wrong
minute. → [Chapter 3](story/outage.md)

**Would widening the watermark help?**
No. Records stamped at publish time are never late. The lever is the clock, not the
tolerance.

**How would you switch to event time?**
Parse `ts` into a timestamp, declare it as the time attribute with a watermark sized to
the longest outage you'll wait for — or set the record timestamp from the device clock at
the connector.

**What does event time cost?**
Held state, later results, and trusting device clocks that drift.

---

## Schema

**Is there a schema?**
Yes, on the output. `CREATE TABLE count_mismatches` registers it in Schema Registry
automatically. The raw topics are deliberately schemaless. → [Chapter 4](story/schema.md)

**Why didn't registering a schema on the raw topic work?**
The reader finds the schema through an ID inside each record. `ByteArrayConverter`
doesn't write one. No ID, no schema.

**Why not just use AvroConverter?**
It can only serialize the structure it's handed, and the MQTT source hands it bytes. You
get a one-column primitive schema. Real fields need a parsing transform first.

**Why not Avro on the devices?**
The producer would need Schema Registry credentials and cloud access — on every device.

---

## Correctness

**Inner or outer join, and why does it matter?**
`02` uses inner, so a window with one side empty disappears. Reconciliation needs outer
— the missing side is the signal. → [Windows & joins](concepts/windows-and-joins.md)

**Does the classification work?**
`06` does, because it uses lag as a second signal. `02` mislabels three of five faults
because it only looks at size. → [Fault Scenarios](faults.md)

**Which system is right when they disagree?**
Reconciliation alone can't say — only that they differ and in which direction. You need
a third signal: lag, device status, or a repeating pattern on one machine.
→ [Chapter 5](story/silent-faults.md#a-second-order-problem-who-gets-the-blame)

**Can any of the faults go undetected?**
Yes — clock jitter. It only changes `ts`, the windows don't use `ts`, and the average lag
cancels it out. → [Chapter 5](story/silent-faults.md)

**What's the delivery guarantee?**
At least once end to end. Duplicates show as +1; nothing deduplicates.
→ [Kafka Connect internals](concepts/kafka-connect.md#delivery-guarantees-end-to-end)

**How is per-device ordering kept?**
The MQTT source keys records by MQTT topic, and each device has its own topic.
→ [Partitioning](concepts/partitioning.md)

---

## Honesty

**Is the dashboard live?**
No — it replays the fault timetable, because a cluster running around the clock for a
demo isn't sensible. And it shows some faults the way they were intended, not the way the
pipeline produces them. → [Chapter 6](story/dashboard.md)

**What haven't you done?**
Run Flink in production: restarts under load, savepoints, backfills, schema evolution
against live state. → [Limits](limitations.md)
