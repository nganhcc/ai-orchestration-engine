package com.nganhcc.orchestration.orchestrator.service;

import com.nganhcc.orchestration.orchestrator.dao.JobStepDao;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class StepService {

    private final JobStepDao jobStepDao;
    private final JdbcOperations jdbc;

    public StepService(JobStepDao jobStepDao, JdbcOperations jdbc) {
        this.jobStepDao = jobStepDao;
        this.jdbc = jdbc;
    }

    /**
     * Try to mark step DONE with result JSON if epoch is valid. Also insert an outbox event
     * in the same transaction for downstream publishing.
     * @return true if update applied, false if rejected due to newer epoch
     */
    @Transactional
    public boolean markStepDone(UUID stepId, long epoch, String resultJson) {
        boolean updated = jobStepDao.updateStatusIfEpochAtMost(stepId, epoch, "DONE", resultJson);
        if (!updated) return false;

        // Insert outbox event for cache invalidation / notifications
        String sql = "INSERT INTO outbox(aggregate_id, event_type, payload, published) VALUES(?, ?, CAST(? AS jsonb), false)";
        jdbc.update(sql, stepId, "STEP_DONE_NOTIFY", resultJson);

        // DAG processing: load all steps in the same batch
        // We first need the batchId from the updated step.
        java.util.List<java.util.Map<String, Object>> stepRows = jdbc.queryForList("SELECT batch_id FROM job_step WHERE step_id = ?", stepId);
        if (!stepRows.isEmpty()) {
            UUID batchId = (UUID) stepRows.get(0).get("batch_id");
            java.util.List<com.nganhcc.orchestration.orchestrator.dao.JobStepDao.JobStepRecord> allRecords = jobStepDao.findAllStepsByBatch(batchId);
            
            // Build Map for DagScheduler
            java.util.Map<UUID, DagScheduler.DagStep> stepsMap = new java.util.HashMap<>();
            for (com.nganhcc.orchestration.orchestrator.dao.JobStepDao.JobStepRecord rec : allRecords) {
                java.util.List<UUID> depList = rec.dependsOn() != null ? java.util.Arrays.asList(rec.dependsOn()) : java.util.Collections.emptyList();
                // Overwrite the status of stepId to DONE because DB updated but allRecords read might be concurrent or already updated
                String status = rec.stepId().equals(stepId) ? "DONE" : rec.status();
                stepsMap.put(rec.stepId(), new DagScheduler.DagStep(rec.stepId(), depList, status));
            }

            java.util.List<UUID> unblocked = DagScheduler.unblockDependents(stepsMap, stepId);
            if (!unblocked.isEmpty()) {
                jobStepDao.unblockSteps(unblocked);
            }

            // Check if batch is completed
            boolean allDone = true;
            for (com.nganhcc.orchestration.orchestrator.dao.JobStepDao.JobStepRecord rec : allRecords) {
                String status = rec.stepId().equals(stepId) ? "DONE" : rec.status();
                // Also check if any unblocked steps are now PENDING instead of BLOCKED
                if (unblocked.contains(rec.stepId())) {
                    status = "PENDING";
                }
                if (!"DONE".equals(status)) {
                    allDone = false;
                    break;
                }
            }

            if (allDone) {
                jdbc.update("UPDATE batch_job SET status = 'DONE' WHERE batch_id = ?", batchId);
            }
        }

        return true;
    }
}
