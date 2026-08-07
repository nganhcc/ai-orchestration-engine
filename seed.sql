INSERT INTO batch_job (batch_id, idempotency_key, status, total_documents, priority) VALUES ('00000000-0000-0000-0000-000000000000', 'test', 'PENDING', 1, 5) ON CONFLICT DO NOTHING;
INSERT INTO job_step (step_id, batch_id, document_id, status, leader_epoch) VALUES ('11111111-1111-1111-1111-111111111111', '00000000-0000-0000-0000-000000000000', 'doc-1', 'ASSIGNED', 1) ON CONFLICT DO NOTHING;
