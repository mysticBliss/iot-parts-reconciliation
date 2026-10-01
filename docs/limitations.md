---
description: "What the project is not, known issues in the current build, and what a production deployment would need."
---

# Limits & Next Steps

## What this project is not

- **Not production Flink.** No statement has been restarted under load, upgraded with a
  savepoint, backfilled, or had its schema evolved against live state. Those are where
  most of the real difficulty in stream processing lives.
- **Not real devices.** The edge is a Python simulator on a laptop. Real plant networks
  have firewalls, segmentation and flaky radios that a simulator doesn't.
- **Not measured at scale.** Ten devices at one pulse every two seconds. No throughput,
  latency or cost figures are claimed.

## Known issues in the current build

| Issue | Where | Fix |
|---|---|---|
| Inner join hides empty windows | `02` | `FULL OUTER JOIN` with `COALESCE(…, 0)`, as in `06` |
| Size-only classification mislabels 3 of 5 faults | `02` | Use lag as a second signal, as `06` does — or label by direction only |
| Clock-jitter fault can't affect windows | Simulator + `$rowtime` | Window on event time, or drop the fault |
| Jitter averages out of the lag metric | `06` | `MAX(ABS(lag))` or `STDDEV_POP(lag)` |
| MES timing drift has no effect | `mes_simulator.py` | Delay the `produce()` call, not just `ts` |
| `CLOCK_JITTER` label means "device below MES" | `06` | Rename to describe direction |
| Line membership differs for `pi-04`, `pi-07` | `06` vs `04` / simulators | One mapping, ideally a dimension table |
| Parsing block repeated in every file | `01`, `02` | Define the views once |
| `CAST(… AS INT)` on unvalidated input | All views | `TRY_CAST` |
| Dashboard Architecture tab out of date | `dashboard.py` | Describe the current build |
| Avro connector config wouldn't run as written | `mqtt-source-avro-connector.json` | Needs a parsing SMT ahead of `ValueToKey` |

## What production would need

**At the edge**

- TLS and per-device credentials on Mosquitto; a broker cluster rather than one node.
- A message ID in each payload, so duplicates from QoS 1 retries can be removed.
- A local buffer on the Connect side as well as the device, for longer cloud outages.

**In the stream**

- A decision, made deliberately, between ingestion time and event time — with the
  watermark sized from the measured lag distribution if event time.
- Dimension data (lines, operations, unit costs) in a table that changes without
  redeploying SQL, joined with `FOR SYSTEM_TIME AS OF`.
- A dead-letter path for payloads that don't parse.
- Monitoring on consumer lag, watermark progress and late-record counts.

**At the output**

- A durable sink for the reconciliation log — the earlier PostgreSQL design was
  closer to right on this than the current one.
- Alerting on sustained discrepancy per device, not per window.
- Schema compatibility rules set on `count_mismatches-value`, since consumers depend
  on it.
