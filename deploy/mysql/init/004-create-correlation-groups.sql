CREATE TABLE IF NOT EXISTS alert_candidate_groups (
    group_id CHAR(36) NOT NULL,
    dataset_id CHAR(36) NOT NULL,
    anchor_event_key VARCHAR(191) NOT NULL,
    window_start DATETIME(3) NOT NULL,
    window_end DATETIME(3) NOT NULL,
    snapshot_json JSON NOT NULL,
    investigation_id CHAR(36) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (group_id),
    INDEX idx_candidate_groups_anchor (dataset_id, anchor_event_key, created_at),
    CONSTRAINT fk_candidate_groups_investigation
        FOREIGN KEY (investigation_id) REFERENCES investigations (id)
) ENGINE = InnoDB;
