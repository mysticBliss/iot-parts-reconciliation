-- =============================================================================
-- Phase 8: Complex Event Processing (Flink CEP via MATCH_RECOGNIZE)
-- =============================================================================
-- Simple SQL aggregations (SUM, COUNT) calculate metrics over time boxes.
-- Flink CEP (Complex Event Processing) detects SEQUENTIAL PATTERNS & STATE MACHINES
-- across incoming event streams in real time.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Pattern 1: Rapid Chatter / Sensor Optical Bounce Flapping Detector
-- Detects if a single machine emits 3 or more multi-count pulses (> 1 part)
-- within a 20-second rolling duration (indicates physical optical reflector vibration).
-- -----------------------------------------------------------------------------
SELECT *
FROM v_device_counts
    MATCH_RECOGNIZE (
        PARTITION BY device_id
        ORDER BY event_time
        MEASURES
            FIRST(A.event_time) AS pattern_start,
            LAST(B.event_time) AS pattern_end,
            COUNT(B.parts) + 1 AS consecutive_bounces,
            SUM(B.parts) + FIRST(A.parts) AS total_parts_produced
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A B+) WITHIN INTERVAL '20' SECOND
        DEFINE
            A AS A.parts > 1,
            B AS B.parts > 1
    );


-- -----------------------------------------------------------------------------
-- Pattern 2: Silent Sensor Fault / Consecutive Zero-Pulse Dropping
-- Detects if a machine emits 2 consecutive pulses with parts = 0 within 15 seconds.
-- (Scheduled on pi-06 at MM:15)
-- -----------------------------------------------------------------------------
SELECT *
FROM v_device_counts
    MATCH_RECOGNIZE (
        PARTITION BY device_id
        ORDER BY event_time
        MEASURES
            FIRST(A.event_time) AS failure_start,
            LAST(B.event_time) AS failure_end,
            'CRITICAL: CONSECUTIVE ZERO COUNTS DETECTED' AS alert_msg
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A B) WITHIN INTERVAL '15' SECOND
        DEFINE
            A AS A.parts = 0,
            B AS B.parts = 0
    );


-- -----------------------------------------------------------------------------
-- Pattern 3: Outage Recovery Spike / Rapid Burst Detector
-- Detects when a machine reconnects and emits > 10 pulses within 5 seconds.
-- (Matches pi-03 when it re-flushes buffered backlog after MM:35 outage!)
-- -----------------------------------------------------------------------------
SELECT *
FROM v_device_counts
    MATCH_RECOGNIZE (
        PARTITION BY device_id
        ORDER BY event_time
        MEASURES
            FIRST(BURST.event_time) AS burst_start,
            LAST(BURST.event_time) AS burst_end,
            COUNT(BURST.parts) AS total_burst_events,
            SUM(BURST.parts) AS total_burst_parts,
            'RECOVERY FLUSH DETECTED' AS incident_tag
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (BURST{10,}) WITHIN INTERVAL '5' SECOND
        DEFINE
            BURST AS TRUE
    );
