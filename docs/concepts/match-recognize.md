---
description: "Flink SQL MATCH_RECOGNIZE explained with three IoT patterns — sensor chatter, silent sensor, recovery flush — and why the ordering clock matters."
---

# MATCH_RECOGNIZE

## Why aggregation isn't enough

`SUM` over a window tells you *how many*. It can't tell you *in what order*. "Three
multi-count pulses in a row within 20 seconds" is a sequence, and a window can also cut
it in half at a boundary. `MATCH_RECOGNIZE` matches sequences — regular expressions over
rows.

## Anatomy

From pattern 1 in `05_complex_event_processing.sql`:

```sql
SELECT *
FROM v_device_counts
    MATCH_RECOGNIZE (
        PARTITION BY device_id                 -- (1) a separate matcher per device
        ORDER BY event_time                    -- (2) must be a time attribute, ascending
        MEASURES                               -- (3) what to output per match
            FIRST(A.event_time) AS pattern_start,
            LAST(B.event_time)  AS pattern_end,
            COUNT(B.parts) + 1  AS consecutive_bounces
        ONE ROW PER MATCH                      -- (4) one summary row
        AFTER MATCH SKIP PAST LAST ROW         -- (5) next match starts after this one
        PATTERN (A B+) WITHIN INTERVAL '20' SECOND   -- (6) the regex, with a time limit
        DEFINE                                 -- (7) what each symbol means
            A AS A.parts > 1,
            B AS B.parts > 1
    );
```

| Part | Meaning |
|---|---|
| `PARTITION BY` | Each device has its own independent state machine |
| `ORDER BY` | Rows are fed to the matcher in this order — must be the time attribute |
| `PATTERN` | Regex syntax: `+` one or more, `*` zero or more, `{n,}` at least *n* |
| `DEFINE` | The condition a row must meet to count as that symbol |
| `WITHIN` | Discard a partial match older than this — bounds state |
| `AFTER MATCH SKIP` | Where to resume; `PAST LAST ROW` prevents overlapping matches |

## The three patterns

| Pattern | Regex | Detects | Fires on |
|---|---|---|---|
| Chatter | `A B+`, all `parts > 1`, within 20 s | Sensor bouncing | `pi-02` :05–:07, `pi-07` :48–:50 |
| Silent sensor | `A B`, both `parts = 0`, within 15 s | Consecutive missed pulses | `pi-06` :15–:17 |
| Recovery flush | `BURST{10,}`, any row, within 5 s | A backlog being replayed | `pi-03` at :37 |

## Ordering by ingestion time is what makes the flush detector work

`ORDER BY event_time` is `$rowtime`. When `pi-03` replays 60 buffered pulses 40 ms
apart, they occupy about 2.4 seconds of `$rowtime` — so 10 of them easily fall within
5 seconds.

Ordered by the device's own `ts` instead, those pulses would be 2 seconds apart, as when
they were made. The pattern would never match.

!!! tip "Remember it as"
    **The clock you order by decides what a "burst" is.** On ingestion time, a replay is
    a burst. On event time, it's just normal traffic that arrived late.
