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
        return true;
    }
}
