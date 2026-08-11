package com.nganhcc.orchestration.orchestrator.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class JobStepDao {

    private final org.springframework.jdbc.core.JdbcTemplate jdbc;
    private final Updater updater;

    public interface Updater {
        int update(String sql, Object... args);
    }

    public record JobStepRecord(
            UUID stepId,
            UUID batchId,
            String documentId,
            String status,
            long leaderEpoch,
            UUID[] dependsOn) {
    }

    @Autowired
    public JobStepDao(org.springframework.jdbc.core.JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.updater = jdbc::update;
    }

    // Package-private constructor for tests to inject a fake updater.
    JobStepDao(Updater updater) {
        this.jdbc = null;
        this.updater = updater;
    }

    /**
     * Update job_step status/result and leader_epoch only if provided epoch is >=
     * stored leader_epoch.
     * Returns true if row was updated (i.e., caller won the CAS), false if a newer
     * epoch already present.
     */
    public boolean updateStatusIfEpochAtMost(UUID stepId, long epoch, String status, String resultJson) {
        String sql = "UPDATE job_step SET status = ?, result = CAST(? AS jsonb), leader_epoch = ?, updated_at = now() "
                + "WHERE step_id = ? AND leader_epoch <= ?";
        int rows = updater.update(sql, status, resultJson, epoch, stepId, epoch);
        return rows > 0;
    }

    private UUID[] getUuidArray(java.sql.Array array) throws java.sql.SQLException {
        if (array == null) return null;
        java.util.UUID[] uuids = (java.util.UUID[]) array.getArray();
        return uuids;
    }

    public java.util.List<JobStepRecord> findPendingSteps(int limit) {
        if (jdbc == null)
            return java.util.Collections.emptyList();
        String sql = "SELECT step_id, batch_id, document_id, status, leader_epoch, depends_on FROM job_step WHERE status = 'PENDING' ORDER BY updated_at ASC LIMIT ?";
        return jdbc.query(sql, (rs, rowNum) -> new JobStepRecord(
                UUID.fromString(rs.getString("step_id")),
                UUID.fromString(rs.getString("batch_id")),
                rs.getString("document_id"),
                rs.getString("status"),
                rs.getLong("leader_epoch"),
                getUuidArray(rs.getArray("depends_on"))), limit);
    }

    public boolean markStepAssigned(UUID stepId, long epoch, String workerId) {
        String sql = "UPDATE job_step SET status = 'IN_PROGRESS', assigned_worker_id = ?, leader_epoch = ?, updated_at = now() "
                + "WHERE step_id = ? AND status = 'PENDING' AND leader_epoch <= ?";
        int rows = updater.update(sql, workerId, epoch, stepId, epoch);
        return rows > 0;
    }

    public int reapStaleSteps(long stalenessThresholdSeconds) {
        String sql = "UPDATE job_step SET status = 'PENDING', retry_count = retry_count + 1, updated_at = now() "
                + "WHERE status IN ('ASSIGNED', 'IN_PROGRESS') AND updated_at < (now() - (? || ' seconds')::INTERVAL)";
        return updater.update(sql, String.valueOf(stalenessThresholdSeconds));
    }

    public java.util.List<JobStepRecord> findAllStepsByBatch(UUID batchId) {
        if (jdbc == null) return java.util.Collections.emptyList();
        String sql = "SELECT step_id, batch_id, document_id, status, leader_epoch, depends_on FROM job_step WHERE batch_id = ?";
        return jdbc.query(sql, (rs, rowNum) -> new JobStepRecord(
                UUID.fromString(rs.getString("step_id")),
                UUID.fromString(rs.getString("batch_id")),
                rs.getString("document_id"),
                rs.getString("status"),
                rs.getLong("leader_epoch"),
                getUuidArray(rs.getArray("depends_on"))
        ), batchId);
    }

    public int unblockSteps(java.util.List<UUID> stepIds) {
        if (stepIds.isEmpty()) return 0;
        String sql = "UPDATE job_step SET status = 'PENDING', updated_at = now() WHERE step_id = ANY(?)";
        try {
            java.sql.Connection conn = java.util.Objects.requireNonNull(jdbc.getDataSource()).getConnection();
            java.sql.Array array = conn.createArrayOf("uuid", stepIds.toArray());
            int count = updater.update(sql, array);
            conn.close();
            return count;
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder("UPDATE job_step SET status = 'PENDING', updated_at = now() WHERE step_id IN (");
            for (int i = 0; i < stepIds.size(); i++) {
                sb.append("?");
                if (i < stepIds.size() - 1) sb.append(",");
            }
            sb.append(")");
            return updater.update(sb.toString(), stepIds.toArray());
        }
    }

    public static record BatchPriorityPending(UUID batchId, int priority, int pendingCount) {}

    public java.util.List<BatchPriorityPending> findActiveBatchesWithPendingCount() {
        if (jdbc == null) return java.util.Collections.emptyList();
        String sql = "SELECT b.batch_id, b.priority, COUNT(s.step_id) as pending_cnt " +
                "FROM batch_job b JOIN job_step s ON b.batch_id = s.batch_id " +
                "WHERE b.status = 'RUNNING' AND s.status = 'PENDING' " +
                "GROUP BY b.batch_id, b.priority";
        return jdbc.query(sql, (rs, rowNum) -> new BatchPriorityPending(
                UUID.fromString(rs.getString("batch_id")),
                rs.getInt("priority"),
                rs.getInt("pending_cnt")
        ));
    }

    public java.util.List<JobStepRecord> findPendingStepsForBatches(java.util.List<UUID> batchIds, int limit) {
        if (jdbc == null || batchIds.isEmpty()) return java.util.Collections.emptyList();
        try {
            java.sql.Connection conn = java.util.Objects.requireNonNull(jdbc.getDataSource()).getConnection();
            java.sql.Array array = conn.createArrayOf("uuid", batchIds.toArray());
            String sql = "SELECT step_id, batch_id, document_id, status, leader_epoch, depends_on " +
                    "FROM job_step WHERE status = 'PENDING' AND batch_id = ANY(?) LIMIT ?";
            java.util.List<JobStepRecord> result = jdbc.query(sql, (rs, rowNum) -> new JobStepRecord(
                    UUID.fromString(rs.getString("step_id")),
                    UUID.fromString(rs.getString("batch_id")),
                    rs.getString("document_id"),
                    rs.getString("status"),
                    rs.getLong("leader_epoch"),
                    getUuidArray(rs.getArray("depends_on"))
            ), array, limit);
            conn.close();
            return result;
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder("SELECT step_id, batch_id, document_id, status, leader_epoch, depends_on " +
                    "FROM job_step WHERE status = 'PENDING' AND batch_id IN (");
            for (int i = 0; i < batchIds.size(); i++) {
                sb.append("?");
                if (i < batchIds.size() - 1) sb.append(",");
            }
            sb.append(") LIMIT ?");
            Object[] params = new Object[batchIds.size() + 1];
            for (int i = 0; i < batchIds.size(); i++) {
                params[i] = batchIds.get(i);
            }
            params[batchIds.size()] = limit;
            return jdbc.query(sb.toString(), (rs, rowNum) -> new JobStepRecord(
                    UUID.fromString(rs.getString("step_id")),
                    UUID.fromString(rs.getString("batch_id")),
                    rs.getString("document_id"),
                    rs.getString("status"),
                    rs.getLong("leader_epoch"),
                    getUuidArray(rs.getArray("depends_on"))
            ), params);
        }
    }

    /**
     * Find steps that have the given stepId in their depends_on array.
     */
    public java.util.List<JobStepRecord> findDependentSteps(java.util.UUID stepId) {
        if (jdbc == null) return java.util.Collections.emptyList();
        String sql = "SELECT step_id, batch_id, document_id, status, leader_epoch, depends_on FROM job_step WHERE ? = ANY(depends_on)";
        return jdbc.query(sql, (rs, rowNum) -> new JobStepRecord(
                java.util.UUID.fromString(rs.getString("step_id")),
                java.util.UUID.fromString(rs.getString("batch_id")),
                rs.getString("document_id"),
                rs.getString("status"),
                rs.getLong("leader_epoch"),
                getUuidArray(rs.getArray("depends_on"))
        ), stepId);
    }
            
}
