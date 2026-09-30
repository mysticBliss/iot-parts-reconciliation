-- =============================================================================
-- Phase 7: Dynamic Dimension Enrichment via Flink Temporal / Reference Views
-- =============================================================================
-- In discrete manufacturing, raw IoT telemetry only contains (device_id, parts, ts).
-- Flink enriches the telemetry stream in real-time with 3 critical business dimensions:
--   1. Production Line & Plant Zone
--   2. Manufacturing Operation Stage & Sensor Technology
--   3. Unit Stage Valuation & Financial Accounting Subtypes
-- =============================================================================

-- Step 1: Enriched Telemetry Stream View (Operations & Production Line)
CREATE OR REPLACE VIEW v_enriched_device_telemetry AS
SELECT
    d.event_time,
    d.device_id,
    d.line AS raw_line,
    d.parts,
    d.status,
    d.ts,

    -- 1. Descriptive Production Line
    CASE 
        WHEN d.device_id IN ('pi-01', 'pi-02', 'pi-03', 'pi-04') THEN 'Stamping & Press Line 1'
        WHEN d.device_id IN ('pi-05', 'pi-06', 'pi-07') THEN 'Robotic Welding Line 2'
        ELSE 'Final Assembly Line 3'
    END AS production_line,

    -- 2. Plant Zone
    CASE 
        WHEN d.device_id IN ('pi-01', 'pi-02', 'pi-03', 'pi-04') THEN 'Zone A - Stamping & Press'
        WHEN d.device_id IN ('pi-05', 'pi-06', 'pi-07') THEN 'Zone B - Robotic Welding'
        ELSE 'Zone C - Final Assembly'
    END AS plant_zone,

    -- 3. Manufacturing Operation
    CASE
        WHEN d.device_id = 'pi-01' THEN 'Sheet Metal Blanking'
        WHEN d.device_id = 'pi-02' THEN 'Hydraulic Deep Draw'
        WHEN d.device_id = 'pi-03' THEN 'High-Speed Piercing'
        WHEN d.device_id = 'pi-04' THEN 'Edge Trimming & Deburr'
        WHEN d.device_id = 'pi-05' THEN 'Chassis Spot Welding'
        WHEN d.device_id = 'pi-06' THEN 'MIG Seam Welding'
        WHEN d.device_id = 'pi-07' THEN 'Sub-Frame Alignment'
        WHEN d.device_id = 'pi-08' THEN 'Powertrain Marriage'
        WHEN d.device_id = 'pi-09' THEN 'Electrical Harness Fit'
        WHEN d.device_id = 'pi-10' THEN 'Final Inspection & Packaging'
        ELSE 'General Manufacturing'
    END AS manufacturing_operation,

    -- 4. Hardware Sensor Technology
    CASE 
        WHEN d.device_id IN ('pi-02', 'pi-07') THEN 'Optical Through-Beam (Bounce Prone)'
        WHEN d.device_id = 'pi-06' THEN 'Inductive Proximity (Miss Prone)'
        WHEN d.device_id = 'pi-03' THEN 'Wireless Edge Gateway (Outage Prone)'
        WHEN d.device_id = 'pi-09' THEN 'Legacy PLC (Clock Jitter Prone)'
        ELSE 'Standard Solid-State Relay'
    END AS sensor_tech,

    -- 5. Standard Stage Unit Cost Valuation ($ USD)
    CASE 
        WHEN d.device_id IN ('pi-01', 'pi-02', 'pi-03', 'pi-04') THEN 45.00
        WHEN d.device_id IN ('pi-05', 'pi-06', 'pi-07') THEN 85.00
        ELSE 150.00
    END AS unit_value_usd,

    -- 6. Shift Supervisor
    CASE 
        WHEN d.device_id IN ('pi-01', 'pi-02', 'pi-03', 'pi-04') THEN 'Marcus Vance (Stamping Lead)'
        WHEN d.device_id IN ('pi-05', 'pi-06', 'pi-07') THEN 'Elena Rostova (Robotics Lead)'
        ELSE 'David Kim (Assembly Lead)'
    END AS line_supervisor

FROM v_device_counts d;


-- Step 2: Enriched 1-Minute Reconciliation Stream with Financial Subtypes
CREATE OR REPLACE VIEW v_enriched_reconciliation_mismatches AS
WITH device_agg AS (
    SELECT
        window_start,
        window_end,
        device_id,
        production_line,
        plant_zone,
        manufacturing_operation,
        sensor_tech,
        unit_value_usd,
        line_supervisor,
        SUM(parts) AS device_total,
        AVG(TIMESTAMPDIFF(SECOND, TO_TIMESTAMP_LTZ(ts, 'yyyy-MM-dd''T''HH:mm:ss''Z'''), event_time)) AS avg_lag_sec
    FROM TABLE(
        TUMBLE(
            TABLE v_enriched_device_telemetry,
            DESCRIPTOR(event_time),
            INTERVAL '1' MINUTE
        )
    )
    GROUP BY window_start, window_end, device_id, production_line, plant_zone, manufacturing_operation, sensor_tech, unit_value_usd, line_supervisor
),
system_agg AS (
    SELECT
        window_start,
        window_end,
        device_id,
        SUM(parts) AS system_total
    FROM TABLE(
        TUMBLE(
            TABLE v_system_counts,
            DESCRIPTOR(event_time),
            INTERVAL '1' MINUTE
        )
    )
    GROUP BY window_start, window_end, device_id
)
SELECT
    COALESCE(d.window_start, s.window_start) AS window_start,
    COALESCE(d.window_end, s.window_end) AS window_end,
    COALESCE(d.device_id, s.device_id) AS device_id,
    d.production_line,
    d.plant_zone,
    d.manufacturing_operation,
    d.sensor_tech,
    d.line_supervisor,
    d.unit_value_usd,
    COALESCE(d.device_total, 0) AS device_total,
    COALESCE(s.system_total, 0) AS system_total,
    (COALESCE(d.device_total, 0) - COALESCE(s.system_total, 0)) AS discrepancy,
    ABS(COALESCE(d.device_total, 0) - COALESCE(s.system_total, 0)) * d.unit_value_usd AS financial_exposure_usd,

    -- Technical Issue Taxonomy
    CASE
        WHEN COALESCE(d.device_total, 0) > COALESCE(s.system_total, 0) AND COALESCE(d.avg_lag_sec, 0) > 10 THEN 'BURST_RECOVERY'
        WHEN COALESCE(d.device_total, 0) > COALESCE(s.system_total, 0) THEN 'POSITIVE_BOUNCE'
        WHEN COALESCE(d.device_total, 0) = 0 AND COALESCE(s.system_total, 0) > 0 THEN 'NEGATIVE_MISSED'
        WHEN COALESCE(d.device_total, 0) < COALESCE(s.system_total, 0) THEN 'CLOCK_JITTER'
        ELSE 'NORMAL_SYNC'
    END AS issue_type,

    -- Financial Accounting Subtypes
    CASE
        WHEN COALESCE(d.device_total, 0) > COALESCE(s.system_total, 0) THEN 'PHANTOM_INVENTORY_RISK'
        WHEN COALESCE(d.device_total, 0) = 0 AND COALESCE(s.system_total, 0) > 0 THEN 'UNRECORDED_PRODUCTION_LEAK'
        WHEN COALESCE(d.avg_lag_sec, 0) > 10 THEN 'OUTAGE_BACKLOG_DELAY'
        WHEN COALESCE(d.device_total, 0) < COALESCE(s.system_total, 0) THEN 'WINDOW_TIMING_DRIFT'
        ELSE 'ON_TARGET'
    END AS financial_subtype,

    COALESCE(d.avg_lag_sec, 0) AS avg_lag_sec
FROM device_agg d
FULL OUTER JOIN system_agg s
    ON d.device_id = s.device_id
    AND d.window_start = s.window_start
    AND d.window_end = s.window_end;

