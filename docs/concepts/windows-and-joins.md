---
description: "Flink SQL window TVFs (TUMBLE, HOP, CUMULATE, SESSION), and why reconciliation needs an outer join between windowed streams."
---

# Windows & joins

## Window table-valued functions

Flink SQL windows are written as table-valued functions (TVFs): a function that takes a
table and returns a table with extra columns.

```sql
SELECT device_id, window_start, window_end, SUM(parts) AS device_total
FROM TABLE(
    TUMBLE(
        TABLE v_device_counts,      -- input
        DESCRIPTOR(event_time),     -- which column is the time attribute
        INTERVAL '1' MINUTE         -- window size
    )
)
GROUP BY device_id, window_start, window_end;
```

`TUMBLE` adds `window_start`, `window_end` and `window_time` to every row. Grouping by
`window_start, window_end` gives one result per window, emitted when the watermark
passes `window_end`.

`DESCRIPTOR(event_time)` names a column rather than reading its value. That column must
be a **time attribute** — here, `$rowtime` under an alias. Calling it `event_time`
doesn't make it event time.

## Window types

| Type | Shape | Good for |
|---|---|---|
| `TUMBLE` | Fixed size, no overlap | Per-minute reconciliation — every pulse in exactly one window |
| `HOP` | Fixed size, overlapping (slide < size) | Rolling averages — "last 5 minutes, every minute" |
| `CUMULATE` | Grows in steps until max size, then resets | Running totals within a shift |
| `SESSION` | Closes after a gap of inactivity | Detecting a machine that's gone quiet |

!!! tip "A use for `SESSION` here"
    A device that stops sending is the hardest fault to see — there's nothing to
    aggregate. A session window with a gap of a few minutes closes when the device goes
    quiet, and that close *is* the alert.

## Joining two windowed streams

The reconciliation aggregates each stream into windows, then joins the results on device
and `window_start` / `window_end`. Which join you pick decides what you can see:

| Device window | MES window | `INNER JOIN` | `FULL OUTER JOIN` + `COALESCE(…, 0)` |
|---|---|---|---|
| 30 | 30 | Row: 0 | Row: 0 |
| 75 | 30 | Row: +45 | Row: +45 |
| *(none)* | 30 | **No row** | Row: −30 |
| 30 | *(none)* | **No row** | Row: +30 |

For reconciliation, the rows where one side is missing are the most important ones —
that's a device gone dark, or an MES that stopped recording. **Reconciliation needs an
outer join.** `02` uses an inner join; `04` and `06` use a full outer join.

!!! tip "Remember it as"
    **Inner join: you see what arrived. Outer join: you also see what didn't.**

## State

Window aggregates keep one running total per key per open window, and release it when
the window fires. Joins and non-windowed aggregations can keep state indefinitely, which
is what `SET 'sql.state-ttl' = '1 HOUR'` in `02` bounds. The trade: state older than the
TTL is discarded, so a record that matched something more than an hour old won't.
