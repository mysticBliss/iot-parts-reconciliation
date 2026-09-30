-- =============================================================================
-- Phase 5 & 6: Confluent Cloud Flink SQL DDL Definitions
-- =============================================================================

-- 1. Device Counts Stream Table (Source: Edge IoT devices via Kafka Connect)
CREATE TABLE IF NOT EXISTS device_counts (
    device_id STRING,
    line STRING,
    parts INT,
    ts STRING,
    -- Derive event-time timestamp from ISO-8601 string
    event_time AS TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
    -- Define 10-second bounded out-of-orderness watermark
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'confluent',
    'scan.startup.mode' = 'earliest-offset'
);

-- 2. System Counts Stream Table (Source: MES System of Record)
CREATE TABLE IF NOT EXISTS system_counts (
    device_id STRING,
    line STRING,
    parts INT,
    ts STRING,
    event_time AS TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'confluent',
    'scan.startup.mode' = 'earliest-offset'
);

-- 3. Discrepancy Output Sink Table (Target: count_mismatches topic -> Postgres)
CREATE TABLE IF NOT EXISTS count_mismatches (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    device_id STRING,
    device_total INT,
    system_total INT,
    discrepancy INT
) WITH (
    'connector' = 'confluent'
);
