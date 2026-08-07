package com.nganhcc.orchestration.orchestrator.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class JobStepDao {

    private final Updater updater;

    public interface Updater {
        int update(String sql, Object... args);
    }

    @Autowired
    public JobStepDao(JdbcOperations jdbc) {
        this.updater = jdbc::update;
    }

    // Package-private constructor for tests to inject a fake updater.
    JobStepDao(Updater updater) {
        this.updater = updater;
    }

    /**
     * Update job_step status/result and leader_epoch only if provided epoch is >= stored leader_epoch.
     * Returns true if row was updated (i.e., caller won the CAS), false if a newer epoch already present.
     */
    public boolean updateStatusIfEpochAtMost(UUID stepId, long epoch, String status, String resultJson) {
        String sql = "UPDATE job_step SET status = ?, result = CAST(? AS jsonb), leader_epoch = ?, updated_at = now() "
                + "WHERE step_id = ? AND leader_epoch <= ?";
        int rows = updater.update(sql, status, resultJson, epoch, stepId, epoch);
        return rows > 0;
    }
}
