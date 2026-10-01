---
description: "Two simulated faults that ingestion-time windows cannot detect, why averaging hides clock jitter, and why reconciliation can't say which side is wrong."
---

# 5 · The faults that can't fire

<div class="takeaway" markdown>
**The line to keep**
Injecting a fault doesn't mean your pipeline can see it. Check every fault against the
clock your windows actually run on.
</div>

!!! note "When this was found"
    Not during a run — while writing these docs, by reading the simulator code back
    against the lesson from [Chapter 3](outage.md). It's the same lesson, in two more
    places.

## Clock jitter on `pi-09`

The fault timetable says that at minutes :25–:26, `pi-09`'s clock jumps ±25 seconds,
simulating a PLC that has lost NTP sync. The intended effect: pulses land in the wrong
one-minute window, producing a `+N` / `−N` pair across a window boundary.

Here's what the simulator actually does:

```python
elif curr_minute in [25, 26] and dev == "pi-09":
    ts_offset = random.choice([-25, 25])     # only changes the ts *string*
...
ts = get_iso_timestamp(offset_seconds=ts_offset)
client.publish(topic, payload_str, qos=1)    # still published right now
```

The offset goes into the `ts` field in the payload. The record is still published
immediately, so its Kafka timestamp — `$rowtime` — is correct. And every window keys on
`$rowtime`.

**So the jitter can't move a single pulse into a different window.** `pi-09` shows 30
device parts against 30 MES parts, and the pipeline reports no discrepancy.

### It hides from the lag metric too

The analytics views compute the *average* gap between `ts` and `$rowtime` per window.
The jitter is ±25 seconds chosen at random on every tick — so over a minute, the +25s and
−25s readings cancel out and the average stays near zero.

!!! tip "Remember it as"
    **Averaging a symmetric fault hides it.** To catch jitter you need
    `MAX(ABS(lag))` or a standard deviation, not `AVG(lag)`.

## MES timing drift

The MES simulator has the same issue. Its "timing drift" anomaly shifts the `ts` field
15–45 seconds into the past — but the record is produced immediately, so `$rowtime` is
unaffected and the windows don't move. Of the MES simulator's three anomalies, only two
change the counts:

| MES anomaly | Share of records (at 15%) | Changes window counts? |
|---|---|---|
| Dropped record | 6% | Yes — device ends up *above* MES |
| Timing drift | 4.5% | **No** — `ts` only |
| Multi-count (2–3 parts) | 4.5% | Yes — device ends up *below* MES |

## A second-order problem: who gets the blame?

The classification assumes the **device** is the one that's wrong:

- When the MES drops a record, the device total is higher, so the row is labelled
  `POSITIVE_BOUNCE` — a sensor fault — even though the sensor was right.
- When the MES double-counts, the device total is lower. View `06` labels that
  `CLOCK_JITTER`, which, as shown above, this pipeline can't actually produce.

This isn't really a bug. It's a limit of reconciliation itself: **comparing two sources
tells you they disagree, and in which direction — not which one is right.** Telling the
difference needs a third signal, such as the lag metric (which is how `06` separates
burst recovery from bounce), the device's own status field, or the fact that a pattern
repeats on one machine.

## What would make these faults real

| Fault | Fix |
|---|---|
| Device clock jitter | Window on event time parsed from `ts` ([Chapter 3](outage.md#what-id-change)) — then jitter genuinely spills across boundaries |
| MES timing drift | Same — or delay the *produce* call, so the drift affects the Kafka timestamp |
| Jitter detection | Replace `AVG(lag)` with `MAX(ABS(lag))` or `STDDEV_POP(lag)` |
| Root-cause labels | Rename to describe the *direction* (`DEVICE_OVER`, `DEVICE_UNDER`) and attribute cause only with a second signal |

!!! warning "The dashboard shows the intended behaviour"
    The hosted dashboard draws `pi-09` as a 42 / 18 spillover at minutes :25–:26. That's
    what event-time windows would show — not what this pipeline produces. See
    [Chapter 6](dashboard.md).
