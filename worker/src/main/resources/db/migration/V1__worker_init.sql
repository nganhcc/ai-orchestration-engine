CREATE TABLE IF NOT EXISTS worker_step_status (
    step_id UUID PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    result JSONB,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
