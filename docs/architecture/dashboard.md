---
description: "The Streamlit dashboard's tabs, its data model, replay mode, and known gaps against the current pipeline."
---

# Dashboard

[Open the live dashboard :material-open-in-new:](https://iot-parts-reconciliation-anhrfruzvzhhymc9c9mcwc.streamlit.app/){ .md-button }

`dashboard.py` — Streamlit with Plotly, hosted on Streamlit Community Cloud.

## Tabs

| Tab | Shows |
|---|---|
| Production Stream | Device pulses against the MES target, per-minute discrepancy, issue-type breakdown |
| Manufacturing Operations | Output and discrepancy by line and operation stage |
| Financial Valuation | Discrepancy converted to dollars at $45 / $85 / $150 per unit by line |
| Ingestion Latency | `$rowtime − ts` lag per device; fleet sync reliability |
| Fleet Heatmap | Devices × minutes, coloured by discrepancy |
| Pivot Matrix | Slice by line, operation, subtype, supervisor; CSV export |
| Audit Log | Filterable list of discrepancy rows |
| Pipeline Architecture | Architecture description — **out of date**, see below |

The sidebar has a 15–60 minute lookback, an auto-refresh toggle (every 5 s) and an
equipment filter.

## Where the data comes from

The hosted app has no Kafka consumer. `get_reconciliation_data()` builds the rows from
the fault timetable, using the same columns as `v_reconciliation_analytics` in `06`. So
it's the right *shape* of data; the *values* are the timetable's intent.

[Chapter 6](../story/dashboard.md) lists exactly where those values differ from the real
pipeline — most visibly, the `pi-09` jitter, which the pipeline can't produce.

## Known gaps

- The **Pipeline Architecture** tab describes a PostgreSQL JDBC sink, a temporal join,
  a declared watermark and an `enriched_reconciliation` topic. None of these exist in
  the current build.
- `06` groups lines as `pi-01–03` / `pi-04–06` / `pi-07–10`, while the simulators,
  `04` and the dashboard use `pi-01–04` / `pi-05–07` / `pi-08–10`. So `pi-04` and
  `pi-07` are on different lines depending on which you read.
