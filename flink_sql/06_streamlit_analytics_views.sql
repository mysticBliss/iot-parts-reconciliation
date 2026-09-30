-- =============================================================================
-- Phase 9: Confluent Flink SQL Analytics Views & Streamlit Export Pipeline
-- =============================================================================
-- Purpose: Transform raw device & MES event streams into clean, structured,
-- enriched discrepancy records ready for Streamlit dashboard visualization.
-- =============================================================================

-- 1. Device Counts Continuous Window Aggregation
CREATE OR REPLACE VIEW v_device_minute_totals AS
SELECT
    window_start,
    window_end,
    device_id,
    SUM(parts) AS device_total,
    COUNT(*) AS pulse_count,
    AVG(TIMESTAMPDIFF(SECOND, TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), event_time)) AS avg_ingest_lag_sec
FROM TABLE(
    TUMBLE(
        TABLE v_device_counts,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE
    )
)
GROUP BY window_start, window_end, device_id;


-- 2. MES Expected Counts Continuous Window Aggregation
CREATE OR REPLACE VIEW v_system_minute_totals AS
SELECT
    window_start,
    window_end,
    device_id,
    SUM(parts) AS system_total,
    COUNT(*) AS system_record_count
FROM TABLE(
    TUMBLE(
        TABLE v_system_counts,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE
    )
)
GROUP BY window_start, window_end, device_id;


-- 3. Comprehensive Discrepancy Stream with Root-Cause & Metadata Enrichment
CREATE OR REPLACE VIEW v_reconciliation_analytics AS
SELECT
    COALESCE(d.window_start, s.window_start) AS window_start,
    COALESCE(d.window_end, s.window_end) AS window_end,
    COALESCE(d.device_id, s.device_id) AS device_id,
    
    -- Enriched Topology
    CASE 
        WHEN COALESCE(d.device_id, s.device_id) IN ('pi-01', 'pi-02', 'pi-03') THEN 'Line-1 (Stamping)'
        WHEN COALESCE(d.device_id, s.device_id) IN ('pi-04', 'pi-05', 'pi-06') THEN 'Line-2 (Welding)'
        ELSE 'Line-3 (Assembly)'
    END AS production_line,
    
    CASE 
        WHEN COALESCE(d.device_id, s.device_id) IN ('pi-02', 'pi-07') THEN 'Optical Reflector (Bounce Prone)'
        WHEN COALESCE(d.device_id, s.device_id) = 'pi-06' THEN 'Inductive Sensor (Miss Prone)'
        WHEN COALESCE(d.device_id, s.device_id) = 'pi-03' THEN 'Wireless Gateway (Outage Prone)'
        WHEN COALESCE(d.device_id, s.device_id) = 'pi-09' THEN 'Legacy PLC (Clock Jitter Prone)'
        ELSE 'Solid-State Relay (Standard)'
    END AS sensor_type,

    COALESCE(d.device_total, 0) AS device_total,
    COALESCE(s.system_total, 0) AS system_total,
    (COALESCE(d.device_total, 0) - COALESCE(s.system_total, 0)) AS discrepancy,
    COALESCE(d.avg_ingest_lag_sec, 0) AS avg_ingest_lag_sec,

    -- Precise Taxonomy Classification
    CASE
        WHEN COALESCE(d.device_total, 0) > COALESCE(s.system_total, 0) AND COALESCE(d.avg_ingest_lag_sec, 0) > 10 THEN 'BURST_RECOVERY'
        WHEN COALESCE(d.device_total, 0) > COALESCE(s.system_total, 0) THEN 'POSITIVE_BOUNCE'
        WHEN COALESCE(d.device_total, 0) = 0 AND COALESCE(s.system_total, 0) > 0 THEN 'NEGATIVE_MISSED'
        WHEN COALESCE(d.device_total, 0) < COALESCE(s.system_total, 0) THEN 'CLOCK_JITTER'
        ELSE 'NORMAL_SYNC'
    END AS issue_type,

    CURRENT_TIMESTAMP AS analyzed_at
FROM v_device_minute_totals d
FULL OUTER JOIN v_system_minute_totals s
    ON d.device_id = s.device_id
    AND d.window_start = s.window_start
    AND d.window_end = s.window_end;


-- 4. Discrepancy Stream Feed Query (For Streamlit & Alerting Sinks)
SELECT 
    window_start,
    window_end,
    device_id,
    production_line,
    sensor_type,
    device_total,
    system_total,
    discrepancy,
    avg_ingest_lag_sec,
    issue_type,
    analyzed_at
FROM v_reconciliation_analytics
WHERE discrepancy <> 0;
