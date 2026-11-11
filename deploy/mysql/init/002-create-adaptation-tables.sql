CREATE TABLE IF NOT EXISTS identity_mappings (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    dataset_id CHAR(36) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    source_id VARCHAR(191) NOT NULL,
    service VARCHAR(64) NOT NULL,
    instance VARCHAR(191) NOT NULL,
    version VARCHAR(64) NOT NULL,
    valid_from DATETIME(3) NOT NULL,
    valid_to DATETIME(3) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    INDEX idx_identity_resolution
        (dataset_id, environment, source_id, valid_from, valid_to)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS pending_records (
    event_id CHAR(36) NOT NULL,
    dataset_id CHAR(36) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    source_format VARCHAR(32) NOT NULL,
    source_id VARCHAR(191) NOT NULL,
    batch_id VARCHAR(191) NOT NULL,
    record_locator VARCHAR(191) NOT NULL,
    raw_text MEDIUMTEXT NOT NULL,
    reason VARCHAR(500) NOT NULL,
    rule_version VARCHAR(64) NOT NULL,
    status ENUM('pending', 'resolved') NOT NULL DEFAULT 'pending',
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    resolved_at DATETIME(3) NULL,
    PRIMARY KEY (event_id),
    INDEX idx_pending_problem (dataset_id, source_format, reason, status)
) ENGINE = InnoDB;
