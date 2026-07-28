CREATE TABLE batch_job (
    batch_id UUID PRIMARY KEY,
    idempotency_key VARCHAR(128) UNIQUE,      -- NEW: chống submit trùng do client retry
    status VARCHAR(20) NOT NULL,
    total_documents INT NOT NULL,
    priority INT NOT NULL DEFAULT 5,          -- NEW: 1 (cao) .. 10 (thấp), cho fair scheduling
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE job_step (
    step_id UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES batch_job(batch_id),
    document_id VARCHAR(255) NOT NULL,
    depends_on UUID[],                        -- NEW: DAG dependency, NULL/rỗng nếu là step gốc
    status VARCHAR(20) NOT NULL,              -- PENDING, BLOCKED, ASSIGNED, IN_PROGRESS, DONE, FAILED
    assigned_worker_id VARCHAR(64),
    leader_epoch BIGINT NOT NULL,             -- NEW: fencing — update chỉ hợp lệ nếu epoch >= giá trị này
    result JSONB,
    retry_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE raft_log (
    node_id VARCHAR(64) NOT NULL,
    log_index BIGINT NOT NULL,
    term BIGINT NOT NULL,
    command BYTEA NOT NULL,
    PRIMARY KEY (node_id, log_index)
);

CREATE TABLE raft_snapshot (                  -- NEW
    node_id VARCHAR(64) NOT NULL,
    last_included_index BIGINT NOT NULL,
    last_included_term BIGINT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id)
);

CREATE TABLE outbox (                          -- NEW: transactional outbox, thay 2PC
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,                -- vd: step_id
    event_type VARCHAR(50) NOT NULL,           -- CACHE_INVALIDATE, STEP_DONE_NOTIFY, ...
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_job_step_batch_status ON job_step(batch_id, status);
CREATE INDEX idx_outbox_unpublished ON outbox(published) WHERE NOT published;