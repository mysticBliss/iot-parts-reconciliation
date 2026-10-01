# Learning notes — moved

These notes now live on the documentation site, rewritten against what the code actually
does:

**[mysticbliss.github.io/iot-parts-reconciliation](https://mysticbliss.github.io/iot-parts-reconciliation/)**

| Was here | Now |
|---|---|
| Why the raw topics are `VARBINARY`; how Schema Registry works | [Concepts → Schema Registry & converters](docs/concepts/schema-registry.md), [Story → The schema that did nothing](docs/story/schema.md) |
| Partitioning, offsets, Connect internal topics | [Concepts → Keys, partitions, ordering](docs/concepts/partitioning.md), [Concepts → Kafka Connect internals](docs/concepts/kafka-connect.md) |
| Watermarks; event time vs ingestion time | [Concepts → Time](docs/concepts/time.md), [Story → The outage that didn't behave](docs/story/outage.md) |
| `TUMBLE` vs `generate_series` | [Concepts → Windows & joins](docs/concepts/windows-and-joins.md) |
| Clock jitter and window spillover | [Story → The faults that can't fire](docs/story/silent-faults.md) |
| Enrichment, `MATCH_RECOGNIZE`, dashboard tabs | [How It Works → Flink SQL](docs/architecture/flink-sql.md), [Concepts → MATCH_RECOGNIZE](docs/concepts/match-recognize.md), [How It Works → Dashboard](docs/architecture/dashboard.md) |
| Fault simulation modes | [Fault Scenarios](docs/faults.md) |

## Why it was replaced rather than edited

Several sections described behaviour the pipeline doesn't have — in particular, that the
`pi-03` outage replay was dropped by a 10-second watermark, that event-time windows sort
replayed pulses back into place, and that `pi-09`'s clock jitter spills counts across
window boundaries. The windows run on `$rowtime` (Kafka ingestion time) under
Confluent's default 180 ms watermark, so none of those happen. The site explains what
does happen, and why. The previous version is in git history.
