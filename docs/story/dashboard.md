---
description: "Why the public Streamlit dashboard replays the fault timetable instead of consuming Kafka, and exactly where it differs from the pipeline."
---

# 6 · Why the dashboard is a replay

<div class="takeaway" markdown>
**The line to keep**
The stream processing is real; the hosted page is a replay of it. Say so before anyone
clicks the link.
</div>

## The constraint

A live dashboard needs a running Confluent Cloud cluster, a Connect worker, Mosquitto and
both simulators — around the clock, for a page that gets opened a few times a week.
That's a bill for nothing.

## What the hosted dashboard does

[The Streamlit app](https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/)
has no Kafka consumer. `get_reconciliation_data()` in `dashboard.py` generates the last
30 minutes of windows from the simulators' **fault timetable** — 30 parts per device per
minute, with the scheduled faults overlaid. Because the faults are on a fixed schedule,
the ground truth is known in advance, and what the page shows can be checked against it.

## Where the replay diverges from the pipeline

Writing these docs turned up places where the dashboard shows what the system was
*meant* to do rather than what it does:

| Dashboard shows | Pipeline actually produces | Why |
|---|---|---|
| `pi-09` 42 / 18 at :25–:26, `CLOCK_JITTER` | 30 / 30, no discrepancy | Windows run on `$rowtime` — see [Chapter 5](silent-faults.md) |
| `pi-03` lag 72 s during :35–:36 | No device rows, so `avg_ingest_lag_sec` is `COALESCE`d to 0 | Nothing arrives during an outage, so there's nothing to measure. The high lag appears in :37, on the replayed pulses |
| `pi-03` :35–:36 as `NEGATIVE_MISSED` | Matches the `FULL OUTER JOIN` views (`04`, `06`) — but **not** `count_mismatches`, which uses an inner join | See [Chapter 3](outage.md#where-did-the-deficit-go) |
| No MES noise | 15% random MES anomalies by default | The replay only draws the timetable |

The **Pipeline Architecture** tab also still describes an earlier design: a PostgreSQL
JDBC sink, a `FOR SYSTEM_TIME AS OF` temporal join, and a declared
`WATERMARK FOR event_time`. None of those are in the current code. Enrichment in `04` is
done with `CASE` expressions, and no watermark is declared anywhere.

!!! todo "Open"
    Bring the dashboard into line with the pipeline: draw `pi-09` as no discrepancy (or
    switch the pipeline to event time so the jitter is real), and update the Architecture
    tab.

## Running it against a real cluster

Run the stack locally ([Run It](../running.md)) and the Flink views in `06` produce the
real output. The dashboard is shaped around the columns of
`v_reconciliation_analytics`, so it's the same data model either way — the difference is
only where the rows come from.
