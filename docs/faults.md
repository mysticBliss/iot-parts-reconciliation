---
description: "Ground truth for each injected fault — bounce, missed pulse, clock jitter, outage, double count — versus what each Flink SQL layer actually produces."
---

# Fault Scenarios

The simulators inject a fixed timetable of faults every hour, so the expected output is
known in advance. This page is the ground truth: for each fault, what it's *meant* to
show, and what each part of the pipeline actually produces.

All numbers assume the default 2-second tick — 30 pulses per device per minute, MES
expecting 30 — and `--anomaly-rate 0` so MES noise doesn't blur them.

## The timetable

Minutes taken from the code in `device_simulator.py`:

| Minutes | Device | Fault | What the device sends | Real-world cause |
|---|---|---|---|---|
| :05 – :07 | `pi-02` (line 1) | Double-bounce | `parts` = 2 or 3 every pulse | Vibration / reflective bounce on an optical sensor |
| :15 – :17 | `pi-06` (line 2) | Missed pulse | `parts` = 0 every pulse | Fogged lens, conveyor slip |
| :25 – :26 | `pi-09` (line 3) | Clock jitter | `ts` shifted ±25 s, counts normal | PLC lost NTP sync |
| :35 – :36 | `pi-03` (line 1) | Outage + replay | Nothing; 60 pulses replayed at :37 | Plant Wi-Fi drop |
| :48 – :50 | `pi-07` (line 2) | Overheat double-count | `parts` = 2 every pulse | Relay chatter |

## What each layer actually produces

| Fault | Device vs MES per minute | `02` → `count_mismatches` | `06` → analytics view | `05` pattern |
|---|---|---|---|---|
| `pi-02` bounce | ~75 vs 30 → **+45** | `BURST_RECOVERY` :material-close: | `POSITIVE_BOUNCE` :material-check: | Chatter :material-check: |
| `pi-06` missed | 0 vs 30 → **−30** | `OUTAGE_DROP` :material-close: | `NEGATIVE_MISSED` :material-check: | Silent sensor :material-check: |
| `pi-09` jitter | 30 vs 30 → **0** | `NONE` | `NORMAL_SYNC` | — |
| `pi-03` :35–:36 | no device rows vs 30 | *no row* | `NEGATIVE_MISSED` | — |
| `pi-03` :37 | 90 vs 30 → **+60** | `BURST_RECOVERY` :material-check: | `BURST_RECOVERY` :material-check: (lag > 10 s) | Recovery flush :material-check: |
| `pi-07` double | 60 vs 30 → **+30** | `BURST_RECOVERY` :material-close: | `POSITIVE_BOUNCE` :material-check: | Chatter :material-check: |

:material-check: right label · :material-close: wrong label

## What the matrix says

**`06` gets every fault right that the pipeline can see.** It's the better classifier
because it uses a second signal — lag — alongside the size of the difference.

**`02` gets three of five wrong.** Its thresholds (±15) assume a fault means *one* bad
pulse, which would move a window by 1 or 2. But these faults last a whole minute, and at
30 pulses a minute a sustained per-pulse fault moves the total by 30–45. Size alone
can't tell a bounce from a burst. That matters, because `count_mismatches` is the table
with the registered schema — the one downstream consumers would trust.

**Jitter is invisible everywhere.** The windows run on `$rowtime`, and the averaged lag
cancels out. Only the per-record query in `03` shows it. See
[Chapter 5](story/silent-faults.md).

**The outage deficit is only in `06`.** `02`'s inner join drops the empty minutes. See
[Chapter 3](story/outage.md#where-did-the-deficit-go).

!!! tip "Remember it as"
    **Magnitude tells you how much; it doesn't tell you why.** A classifier built on
    the size of the difference alone breaks as soon as the fault rate changes. Pair it
    with a second signal.

!!! info "Derived from the code, not from a recorded run"
    The `02` and `06` labels above are worked out from the simulator logic and the SQL
    thresholds. The `pi-03` burst was observed in a live run; the rest should be
    confirmed with `dual_simulator.py --anomaly-rate 0` against a cluster.

## Clean baseline

```bash
python simulators/dual_simulator.py --clean
```

No timetable, no random glitches, no MES noise. Every window should reconcile to zero —
`02` writes `NONE` rows, `06` shows `NORMAL_SYNC`. Run this first: if the baseline
isn't clean, nothing else on this page means anything.
