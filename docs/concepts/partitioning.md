---
description: "Kafka keys, partitions and ordering: how each topic is keyed, why per-device order holds, and what would break it."
---

# Keys, partitions, ordering

## The rule

A topic is split into partitions. **Kafka guarantees order within a partition, never
across partitions.** The record key decides the partition: records with the same key
go to the same partition, by a hash of the key modulo the partition count.

## How each topic is keyed

| Topic | Key | Set by | Result |
|---|---|---|---|
| `device_counts` | MQTT topic, e.g. `factory/line1/pi-03/parts` | MQTT source connector | All of one device's pulses in one partition, in order |
| `system_counts` | `device_id`, e.g. `pi-03` | MES producer (`key=dev`) | All of one device's MES records in one partition |

Both topics have 3 partitions, so 10 devices share 3 partitions — several devices per
partition, but never one device across two.

!!! info "No `ValueToKey` needed"
    It's easy to assume the key has to be set with a `ValueToKey` transform on
    `device_id`. Here it doesn't: the MQTT source already keys by MQTT topic —
    *"The key is the topic the message was written to"* — and the topic is per device.
    (`ValueToKey` would fail anyway on a bytes value; it needs a struct or map.)

## Why ordering matters here

- **`MATCH_RECOGNIZE`** partitions by `device_id` and walks each device's rows in time
  order. Per-device ordering in Kafka is what makes that order meaningful.
- **Watermarks** are calculated per partition. Records within a partition arriving in
  order keeps the default 180 ms tolerance safe.

## What would break it

| Change | Effect |
|---|---|
| Increasing partition count | `hash(key) % n` changes — a device's new records go to a different partition from its old ones. Ordering across the change is lost |
| Null keys | Records spread across partitions; per-device order gone |
| Keying by line instead of device | Fewer, hotter partitions; one busy line can't spread out |
