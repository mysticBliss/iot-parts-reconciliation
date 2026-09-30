-- =============================================================================
-- Confluent Cloud Flink SQL: Enterprise Multi-Line Reconciliation (Production Grade)
-- Accurately classifies discrepancy problem types:
--   - 'NONE'           : No issue / Perfect Match (discrepancy = 0)
--   - 'BURST_RECOVERY' : High positive burst after outage (>= +15 parts)
--   - 'POSITIVE_BOUNCE': Optical double bounce / chatter (1 to 14 parts over)
--   - 'OUTAGE_DROP'    : Severe connection drop (<= -15 parts under)
--   - 'NEGATIVE_MISSED': Dropped pulse / optical blindness (-1 to -14 parts under)
-- =============================================================================

-- 1. Set State TTL
SET 'sql.state-ttl' = '1 HOUR';

-- 2. Ensure Views Exist
CREATE OR REPLACE VIEW v_device_counts AS
SELECT
    $rowtime AS event_time,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 0) AS device_id,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 1) AS line,
    CAST(SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 2) AS INT) AS parts,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 3) AS status,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 4) AS ts
FROM device_counts;

CREATE OR REPLACE VIEW v_system_counts AS
SELECT
    $rowtime AS event_time,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 0) AS device_id,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 1) AS line,
    CAST(SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 2) AS INT) AS parts,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 3) AS status,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 4) AS ts
FROM system_counts;

-- =============================================================================
-- 3. 1-Minute Tumbling Window Stream-to-Stream Reconciliation Join
-- =============================================================================
INSERT INTO count_mismatches
SELECT
    d.window_start,
    d.window_end,
    d.device_id,
    d.device_total,
    s.system_total,
    (d.device_total - s.system_total) AS discrepancy,
    CASE
        WHEN (d.device_total - s.system_total) = 0    THEN 'NONE'
        WHEN (d.device_total - s.system_total) >= 15  THEN 'BURST_RECOVERY'
        WHEN (d.device_total - s.system_total) > 0    THEN 'POSITIVE_BOUNCE'
        WHEN (d.device_total - s.system_total) <= -15 THEN 'OUTAGE_DROP'
        WHEN (d.device_total - s.system_total) < 0    THEN 'NEGATIVE_MISSED'
        ELSE 'NONE'
    END AS issue_type,
    CURRENT_TIMESTAMP AS detected_at
FROM (
    SELECT
        device_id,
        line,
        window_start,
        window_end,
        SUM(parts) AS device_total
    FROM TABLE(TUMBLE(TABLE v_device_counts, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY device_id, line, window_start, window_end
) d
JOIN (
    SELECT
        device_id,
        line,
        window_start,
        window_end,
        SUM(parts) AS system_total
    FROM TABLE(TUMBLE(TABLE v_system_counts, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY device_id, line, window_start, window_end
) s
  ON d.device_id = s.device_id
 AND d.line = s.line
 AND d.window_start = s.window_start
 AND d.window_end = s.window_end;
