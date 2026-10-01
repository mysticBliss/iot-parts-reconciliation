---
description: "Event time vs ingestion time vs processing time, how Flink watermarks work, and Confluent Cloud's default SOURCE_WATERMARK strategy."
---

# Time: event, ingestion, watermarks

## Three clocks

| Clock | Meaning | In this project |
|---|---|---|
| **Event time** | When the thing happened | `ts` in the payload — the device's clock |
| **Ingestion time** | When the record entered Kafka | `$rowtime` — set when Connect (or the MES producer) publishes |
| **Processing time** | When Flink happens to be looking at it | `CURRENT_TIMESTAMP` — used in `03` experiment 1 and for `detected_at` |

Normally the three are within a second of each other and the difference doesn't matter.
It matters when something delays data: an outage, a backlog, a slow consumer, a clock
that's wrong.

## What a watermark is

A stream never ends, so a window can never be sure it has seen everything. A watermark
is Flink's statement: **"I don't expect any more records older than time *W*."** When the
watermark passes the end of a window, the window fires and emits its result.

Records that arrive with a timestamp *behind* the watermark are **late**. By default
they're dropped from windowed aggregates.

```
timestamps arriving:  19:00:58   19:01:02   19:00:59   19:01:07
max seen so far:      19:00:58   19:01:02   19:01:02   19:01:07
watermark (−5 s):     19:00:53   19:00:57   19:00:57   19:01:02  ← passes 19:01:00
                                                                   window [19:00, 19:01) fires
```

The tolerance is the trade: wider = fewer late records dropped but results come later
and more state is held; narrower = fast results but more drops.

## Confluent Cloud's default

> the SOURCE_WATERMARK function calculates the watermark as the maximum event time seen
> so far in a Kafka partition, minus a fixed out-of-orderness tolerance of 180
> milliseconds.

- Applied to `$rowtime`, per Kafka partition.
- The table's watermark is the **minimum** across its partitions.
- Override at table level: `ALTER TABLE t MODIFY WATERMARK FOR <col> AS <col> - INTERVAL 'n' SECOND`.

## Why it mattered here

The windows key on `$rowtime`. A record's `$rowtime` is when it was *published*, so a
record can only be late if publishing itself happens out of order — which, within a
partition, it barely does. **Delayed data isn't late under ingestion time; it's just
counted in the wrong minute.**

| Situation | Ingestion time (`$rowtime`) | Event time (`ts`) |
|---|---|---|
| Device offline 2 min, then replays | Counted in the recovery minute | Counted in the right minutes — *if* the watermark waits long enough; dropped if not |
| Device clock 25 s off | No effect on windows | Pulses shift into neighbouring windows |
| Device clock badly wrong (hours) | No effect | Records dropped as late, or windows held open |
| Cost | Nothing extra | Held state, delayed results, trust in device clocks |

!!! tip "Remember it as"
    **Watermarks only matter for the clock you window on.** On ingestion time, the
    watermark tolerance is almost irrelevant — the interesting decision is upstream of
    it.

## Measuring the gap between clocks

The analytics views compute it per window:

```sql
AVG(TIMESTAMPDIFF(SECOND, TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), event_time))
```

In steady state it's about a second. On replayed pulses it's 60–120 s. If you moved to
event time, the distribution of this number is what you'd size the watermark from — the
gap you're prepared to wait for.
