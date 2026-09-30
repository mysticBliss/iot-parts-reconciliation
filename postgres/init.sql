-- PostgreSQL Initialization for IoT Parts Reconciliation & Superset

CREATE TABLE IF NOT EXISTS count_mismatches (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP,
    device_id VARCHAR(50) NOT NULL,
    device_total INTEGER NOT NULL,
    system_total INTEGER NOT NULL,
    discrepancy INTEGER NOT NULL,
    issue_type VARCHAR(50),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mismatches_device ON count_mismatches(device_id);
CREATE INDEX IF NOT EXISTS idx_mismatches_window ON count_mismatches(window_start);
CREATE INDEX IF NOT EXISTS idx_mismatches_issue ON count_mismatches(issue_type);
