CREATE TABLE IF NOT EXISTS services (
    dataset_id CHAR(36) NOT NULL,
    service VARCHAR(64) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    topology_json JSON NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (dataset_id, service)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS deployments (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    dataset_id CHAR(36) NOT NULL,
    service VARCHAR(64) NOT NULL,
    version VARCHAR(32) NOT NULL,
    deployed_at DATETIME(3) NOT NULL,
    summary VARCHAR(500) NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_deployments_service_time (dataset_id, service, deployed_at),
    CONSTRAINT fk_deployments_service
        FOREIGN KEY (dataset_id, service)
        REFERENCES services (dataset_id, service)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS investigations (
    id CHAR(36) NOT NULL,
    dataset_id CHAR(36) NOT NULL,
    question TEXT NOT NULL,
    service VARCHAR(64) NOT NULL,
    window_start DATETIME(3) NOT NULL,
    window_end DATETIME(3) NOT NULL,
    status ENUM('queued', 'running', 'completed', 'failed', 'interrupted') NOT NULL,
    error_reason TEXT NULL,
    report_json JSON NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    finished_at DATETIME(3) NULL,
    PRIMARY KEY (id),
    INDEX idx_investigations_service_time (dataset_id, service, created_at)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS alerts (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_key VARCHAR(191) NOT NULL,
    fingerprint VARCHAR(128) NOT NULL,
    started_at DATETIME(3) NOT NULL,
    resolved_at DATETIME(3) NULL,
    status ENUM('firing', 'resolved') NOT NULL,
    labels_json JSON NOT NULL,
    investigation_id CHAR(36) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_alerts_event_key (event_key),
    CONSTRAINT fk_alerts_investigation
        FOREIGN KEY (investigation_id)
        REFERENCES investigations (id)
) ENGINE = InnoDB;
