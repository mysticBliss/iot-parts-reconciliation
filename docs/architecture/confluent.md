---
description: "How Confluent Cloud Flink sees raw topics, what $rowtime is, the default 180 ms watermark, and which Schema Registry subjects exist."
---

# Confluent Cloud topics

## How Flink sees a topic

In Confluent Cloud for Apache Flink, every Kafka topic shows up as a table automatically.
What columns it gets depends on whether the records carry a Schema Registry schema ID:

| Records written with… | Flink table |
|---|---|
| A Schema Registry serializer (Avro, Protobuf, JSON Schema) | Typed columns from the schema |
| Anything else — like `ByteArrayConverter` | `key VARBINARY`, `val VARBINARY`, `'value.format' = 'raw'` |

Both raw topics here are the second kind. Every table also gets a system column,
`$rowtime`.

## `$rowtime`

> `$rowtime` … is exactly the Kafka record timestamp.
> — Confluent Cloud docs

For `device_counts` that's when the Connect worker published; for `system_counts`, when
the MES producer sent it. It is the clock all six SQL files window on.

## Watermarks

Nothing in `flink_sql/` declares a watermark, so the tables use Confluent's default:

> the SOURCE_WATERMARK function calculates the watermark as the maximum event time seen
> so far in a Kafka partition, minus a fixed out-of-orderness tolerance of 180
> milliseconds.

Two practical consequences:

- **Per partition.** With 3 partitions per topic, the table's watermark is the
  *minimum* across partitions, so one quiet partition can hold windows open for all of
  them. Here all three partitions carry traffic, so it doesn't bite.
- **180 ms is tight** — fine here, because `$rowtime` on each partition is close to
  in order. It would not be fine for device time.

To change it, Confluent sets watermarks at table level:

```sql
ALTER TABLE device_counts
  MODIFY WATERMARK FOR $rowtime AS $rowtime - INTERVAL '10' SECOND;
```

## Schema Registry

| Subject | Registered by | Used? |
|---|---|---|
| `count_mismatches-value` | `CREATE TABLE count_mismatches` in `01` | Yes — the typed output contract |
| `device_counts-value` | Registered by hand during the build | No — records carry no schema ID ([Chapter 4](../story/schema.md)) |

> The CREATE TABLE statement always creates a backing Kafka topic as well as the
> corresponding schema subjects for key and value in Schema Registry.
> — Confluent Cloud docs

## State

`02_windowed_reconciliation.sql` sets `'sql.state-ttl' = '1 HOUR'` for its statement.
Window aggregates release their state when the window fires; the TTL bounds anything
that isn't window-scoped, so a long-running statement can't grow state without limit.
