-- Migration V3: Add indexes for DAG scheduling and priority scheduling
CREATE INDEX IF NOT EXISTS idx_job_step_batch_blocked ON job_step(batch_id, status) WHERE status = 'BLOCKED';
CREATE INDEX IF NOT EXISTS idx_batch_job_running ON batch_job(status, priority) WHERE status = 'RUNNING';
