-- =============================================================================
-- Phase 6: Late Data, Clock Drift & Watermark Tolerance Experiments
-- =============================================================================
-- In event-time streaming, the Watermark is the engine's clock asserting:
-- "No more events with a timestamp <= (Current_Watermark) are expected."
--
-- Real-World Challenge:
-- Machine 'pi-03' experiences network drops at MM:35 to MM:37, buffers 60+ pulses,
-- and dumps them in a late-replay burst upon reconnecting at MM:37.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Experiment 1: Real-Time Event-Time vs Ingestion Watermark Lag Monitor
-- -----------------------------------------------------------------------------
-- Compare the device's physical creation timestamp (ts) against Flink's rowtime ($rowtime).
-- This reveals latency spikes and burst replays in real time.
SELECT
    device_id,
    line,
    ts AS edge_device_timestamp,
    event_time AS flink_event_time,
    TIMESTAMPDIFF(SECOND, TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), CURRENT_TIMESTAMP) AS edge_to_cloud_lag_seconds,
    parts,
    status
FROM v_device_counts
WHERE device_id IN ('pi-03', 'pi-09')  -- pi-03 has outage at MM:35, pi-09 has jitter at MM:25
/*+ OPTIONS('scan.startup.mode'='latest-offset') */;


-- -----------------------------------------------------------------------------
-- Experiment 2: Strict Watermark (5s) Aggregation vs Delayed Burst
-- -----------------------------------------------------------------------------
-- With a standard low-latency 5s watermark, late-arriving events (>5s older than
-- highest watermark seen) are ignored in window calculations.
-- Watch minute 35-37 on pi-03:
SELECT
    window_start,
    window_end,
    device_id,
    line,
    SUM(parts) AS window_total_parts,
    COUNT(*) AS total_pulses_counted
FROM TABLE(
    TUMBLE(
        TABLE v_device_counts,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE
    )
)
WHERE device_id = 'pi-03'
GROUP BY window_start, window_end, device_id, line;
