-- =============================================================================
-- Phase 6: Late Data & Watermark Tolerance Experiments
-- =============================================================================

-- Scenario A: Strict Watermark (5-second tolerance)
-- Demonstrates: Fast emission, low state retention, but drops events when machines suffer network lag.
/*
CREATE TABLE device_counts_strict (
    device_id STRING,
    line STRING,
    parts INT,
    ts STRING,
    event_time AS TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
);
*/

-- Scenario B: Relaxed Watermark (60-second tolerance)
-- Demonstrates: Absorbs transient plant power hiccups and device reconnection bursts,
-- with the tradeoff of holding window state for +60s before emitting results.
/*
CREATE TABLE device_counts_relaxed (
    device_id STRING,
    line STRING,
    parts INT,
    ts STRING,
    event_time AS TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
    WATERMARK FOR event_time AS event_time - INTERVAL '60' SECOND
);
*/

-- Scenario C: Idle Machine Detection (Session Window / Inactivity query)
-- Detects if a machine has produced 0 events for > 3 minutes (Silent Failure)
SELECT
    device_id,
    SESSION_START(event_time, INTERVAL '3' MINUTE) AS session_start,
    SESSION_END(event_time, INTERVAL '3' MINUTE) AS session_end,
    COUNT(*) AS event_count
FROM device_counts
GROUP BY
    device_id,
    SESSION(event_time, INTERVAL '3' MINUTE);
