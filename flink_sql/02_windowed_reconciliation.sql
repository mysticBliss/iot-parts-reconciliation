-- =============================================================================
-- Confluent Cloud Flink SQL: Enterprise Multi-Line Reconciliation
-- Parses Delimited format (<device_id>::<line>::<parts>::<status>::<ts>)
-- =============================================================================

-- Step 1: Create structured views parsing the '::' delimiter
CREATE TEMPORARY VIEW v_device_counts AS
SELECT
    $rowtime AS event_time,
    SPLIT_INDEX(CAST(`val` AS STRING), '::', 0) AS device_id,
    SPLIT_INDEX(CAST(`val` AS STRING), '::', 1) AS line,
    CAST(SPLIT_INDEX(CAST(`val` AS STRING), '::', 2) AS INT) AS parts,
    SPLIT_INDEX(CAST(`val` AS STRING), '::', 3) AS status,
    SPLIT_INDEX(CAST(`val` AS STRING), '::', 4) AS ts
FROM device_counts;

CREATE TEMPORARY VIEW v_system_counts AS
SELECT
    $rowtime AS event_time,
    SPLIT_INDEX(CAST(`val` AS STRING), '::', 0) AS device_id,
    SPLIT_INDEX(CAST(`val` AS STRING), '::', 1) AS line,
    CAST(SPLIT_INDEX(CAST(`val` AS STRING), '::', 2) AS INT) AS parts,
    SPLIT_INDEX(CAST(`val` AS STRING), '::', 3) AS status,
    SPLIT_INDEX(CAST(`val` AS STRING), '::', 4) AS ts
FROM system_counts;

-- Step 2: 1-Minute Tumbling Window Multi-Line Reconciliation Stream
-- Reconciles per-machine and per-line production counts
SELECT
    d.window_start,
    d.window_end,
    d.line,
    d.device_id,
    d.device_total,
    s.system_total,
    (d.device_total - s.system_total) AS discrepancy
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
WHERE d.device_total <> s.system_total;
