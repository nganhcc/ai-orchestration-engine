CREATE TABLE raft_meta (
    node_id VARCHAR(64) PRIMARY KEY,
    current_term BIGINT NOT NULL,
    voted_for VARCHAR(64),
    commit_index BIGINT NOT NULL DEFAULT 0,
    last_applied BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
