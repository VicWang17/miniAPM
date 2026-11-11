CREATE TABLE IF NOT EXISTS cases (
    id CHAR(36) NOT NULL,
    dataset_id CHAR(36) NOT NULL,
    service VARCHAR(64) NOT NULL,
    alert_fingerprint VARCHAR(128) NULL,
    investigation_id CHAR(36) NOT NULL,
    summary VARCHAR(500) NOT NULL,
    root_cause TEXT NOT NULL,
    resolution TEXT NOT NULL,
    result_evidence TEXT NULL,
    keywords JSON NOT NULL,
    status ENUM('pending', 'approved', 'rejected') NOT NULL DEFAULT 'pending',
    reject_reason VARCHAR(500) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    approved_at DATETIME(3) NULL,
    hits INT UNSIGNED NOT NULL DEFAULT 0,
    helped INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_cases_investigation (investigation_id),
    INDEX idx_cases_lookup (dataset_id, service, status),
    CONSTRAINT fk_cases_investigation
        FOREIGN KEY (investigation_id)
        REFERENCES investigations (id)
) ENGINE = InnoDB;
