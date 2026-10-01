---
description: "Six chapters on rebuilding a 2018 IoT reconciliation pipeline on Confluent Cloud and Flink SQL, each with the one lesson worth keeping."
---

# The Story

Six short chapters, in the order things happened. Each one opens with the single line
worth keeping, then tells you how I got there.

| # | Chapter | The line to keep |
|---|---|---|
| 1 | [The original, Delphi 2018](origin.md) | Same problem, seven years apart — which makes it a fair comparison. |
| 2 | [The rebuild](rebuild.md) | Every component was chosen by where it has to *run*, not by what it can do. |
| 3 | [The outage that didn't behave](outage.md) | I built an ingestion-time pipeline with an event-time column name. |
| 4 | [The schema that did nothing](schema.md) | Schema Registry is a producer-side contract, not a parser. |
| 5 | [The faults that can't fire](silent-faults.md) | Injecting a fault doesn't mean your pipeline can see it. |
| 6 | [Why the dashboard is a replay](dashboard.md) | Disclose the shortcut before anyone clicks on it. |

!!! tip "How to use these"
    Chapters 3, 4 and 5 are the ones with real findings. Each follows the same shape —
    **what I expected → what happened → why → what I'd change** — and that shape is
    the whole point. A clean result teaches less than a wrong assumption caught and
    explained.
