---
description: "Step-by-step guide to running the pipeline: Confluent Cloud setup, Docker edge stack, connector deployment, simulators, Flink SQL and teardown."
---

# Run It

## You'll need

- Docker with Compose
- Python 3.9+
- A Confluent Cloud cluster with an API key, and a Flink compute pool in the same region

!!! warning "Confluent Cloud bills while it runs"
    Stop the Flink statements and delete the compute pool when you're done. The
    [teardown](#teardown) section covers it.

## 1 · Configure

```bash
cp .env.example .env
```

Fill in `CONFLUENT_BOOTSTRAP_SERVERS`, `CONFLUENT_API_KEY` and `CONFLUENT_API_SECRET`.
`.env` is git-ignored — keep it that way.

```bash
pip install -r requirements.txt
```

## 2 · Create topics

```bash
python scripts/create_topics.py
```

Creates `device_counts`, `system_counts`, `count_mismatches` and `device_counts_dlq`,
3 partitions each.

## 3 · Start the edge

```bash
docker compose up -d --build
```

| Service | Port |
|---|---|
| Mosquitto | `1883` |
| Kafka Connect REST | `8083` |

Check the worker is up: `curl http://localhost:8083/`

## 4 · Deploy the connector

=== "PowerShell"

    ```powershell
    cd scripts
    .\deploy_connectors.ps1
    ```

=== "Bash"

    ```bash
    ./scripts/deploy_connectors.sh
    ```

=== "Python"

    ```bash
    python scripts/deploy_connectors.py
    ```

Check: `curl http://localhost:8083/connectors/mqtt-source-device-counts/status` should
show `RUNNING`.

## 5 · Run the simulators

```bash
python simulators/dual_simulator.py --clean   # baseline: everything should reconcile
python simulators/dual_simulator.py           # with the hourly fault timetable
```

Useful flags:

| Flag | Effect |
|---|---|
| `--clean` | No faults, no MES noise |
| `--no-schedule` | Disable the timetable only |
| `--glitch-rate 0.1` | Random edge glitches |
| `--anomaly-rate 0` | No MES noise — use with the timetable to check [Fault Scenarios](faults.md) |
| `--simulate-outage-device pi-03 --outage-duration 70` | On-demand outage |
| `--rate 1.5` | Seconds per tick (changes the per-minute totals) |

## 6 · Run the Flink SQL

In the Confluent Cloud Flink workspace, run in order:

1. `01_create_tables.sql` — views and the output table
2. `02_windowed_reconciliation.sql` — the continuous reconciliation (long-running)
3. `03`–`06` as needed — experiments, enrichment, patterns, analytics views

Then watch:

```sql
SELECT * FROM count_mismatches WHERE issue_type <> 'NONE';
```

## 7 · Dashboard

```bash
streamlit run dashboard.py
```

## Teardown

Between runs, to start from empty topics:

```bash
python scripts/reset_cluster.py   # deletes the topics and recreates them, 3 partitions each
```

When you're finished:

```bash
docker compose down
```

Then in Confluent Cloud: stop the long-running Flink statements, delete the compute
pool, and delete the topics (or the cluster) if you don't need them.
