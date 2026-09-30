-- =============================================================================
-- Phase 5 & 6: Confluent Cloud Flink SQL DDL Definitions
-- Includes issue_type classification: 'POSITIVE_BOUNCE', 'NEGATIVE_MISSED', 'BURST_RECOVERY', 'CLOCK_JITTER'
-- =============================================================================

-- 1. Device Counts Stream View
CREATE OR REPLACE VIEW v_device_counts AS
SELECT
    $rowtime AS event_time,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 0) AS device_id,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 1) AS line,
    CAST(SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 2) AS INT) AS parts,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 3) AS status,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 4) AS ts
FROM device_counts;

-- 2. System Counts Stream View
CREATE OR REPLACE VIEW v_system_counts AS
SELECT
    $rowtime AS event_time,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 0) AS device_id,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 1) AS line,
    CAST(SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 2) AS INT) AS parts,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 3) AS status,
    SPLIT_INDEX(MAKE_VALID_UTF8(`val`), '::', 4) AS ts
FROM system_counts;

-- 3. Discrepancy Output Sink Table (with issue_type column)
DROP TABLE IF EXISTS `count_mismatches`;

CREATE TABLE `count_mismatches` (
    `window_start` TIMESTAMP(3),
    `window_end` TIMESTAMP(3),
    `device_id` STRING,
    `device_total` INT,
    `system_total` INT,
    `discrepancy` INT,
    `issue_type` STRING,
    `detected_at` TIMESTAMP_LTZ(3)
);
