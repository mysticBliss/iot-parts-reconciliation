---
description: "Revision notes on the streaming concepts behind the project, each tied to where it shows up in the pipeline."
---

# Concepts

The background each part of the project depends on — written as revision notes, and
tied back to where it shows up in this pipeline.

| Page | One-line version |
|---|---|
| [Time: event, ingestion, watermarks](time.md) | Every record has more than one timestamp; your windows use exactly one of them. |
| [Windows & joins](windows-and-joins.md) | A window decides *which bucket*; the join type decides *whether an empty bucket shows up*. |
| [Schema Registry & converters](schema-registry.md) | The schema ID travels inside each record — no ID, no schema. |
| [Kafka Connect internals](kafka-connect.md) | Connectors move data, converters encode it, transforms reshape it, and state lives in Kafka. |
| [Keys, partitions, ordering](partitioning.md) | Order is only guaranteed within a partition; the key picks the partition. |
| [MATCH_RECOGNIZE](match-recognize.md) | Regular expressions over a stream of rows. |
