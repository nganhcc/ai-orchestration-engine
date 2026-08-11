package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.LogEntry;
import com.nganhcc.orchestration.raftcore.RaftLogStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implement RaftLogStore bằng JDBC lên PostgreSQL (bảng raft_log / raft_snapshot).
 * Được gọi TỪ TRONG khối synchronized của RaftLog — không gọi ngược lại RaftLog.
 */
public final class JdbcRaftLogStore implements RaftLogStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcRaftLogStore.class);

    private final JdbcTemplate jdbc;

    public JdbcRaftLogStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void append(String nodeId, LogEntry entry) {
        try {
            jdbc.update(
                "INSERT INTO raft_log(node_id, log_index, term, command) VALUES (?, ?, ?, ?) "
                    + "ON CONFLICT (node_id, log_index) DO NOTHING",
                nodeId, entry.index(), entry.term(), entry.command()
            );
        } catch (Exception e) {
            log.warn("raft.log.append.failed nodeId={} index={} err={}", nodeId, entry.index(), e.getMessage());
        }
    }

    @Override
    public void truncateFrom(String nodeId, long fromIndex) {
        try {
            jdbc.update("DELETE FROM raft_log WHERE node_id = ? AND log_index >= ?", nodeId, fromIndex);
        } catch (Exception e) {
            log.warn("raft.log.truncate.failed nodeId={} fromIndex={} err={}", nodeId, fromIndex, e.getMessage());
        }
    }

    @Override
    public void compactUpTo(String nodeId, long lastIncludedIndex, long lastIncludedTerm) {
        try {
            jdbc.update("DELETE FROM raft_log WHERE node_id = ? AND log_index <= ?", nodeId, lastIncludedIndex);
            saveSnapshot(nodeId, lastIncludedIndex, lastIncludedTerm, new byte[0]);
        } catch (Exception e) {
            log.warn("raft.log.compact.failed nodeId={} lastIncludedIndex={} err={}", nodeId, lastIncludedIndex, e.getMessage());
        }
    }

    @Override
    public List<LogEntry> load(String nodeId) {
        try {
            List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT log_index, term, command FROM raft_log WHERE node_id = ? ORDER BY log_index", nodeId);
            List<LogEntry> result = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                long index = ((Number) row.get("log_index")).longValue();
                long term = ((Number) row.get("term")).longValue();
                byte[] command = (byte[]) row.get("command");
                result.add(new LogEntry(term, index, command));
            }
            return result;
        } catch (Exception e) {
            log.warn("raft.log.load.failed nodeId={} err={}", nodeId, e.getMessage());
            return List.of();
        }
    }

    @Override
    public SnapshotMeta loadSnapshot(String nodeId) {
        try {
            List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT last_included_index, last_included_term FROM raft_snapshot WHERE node_id = ?", nodeId);
            if (rows.isEmpty()) {
                return null;
            }
            Map<String, Object> row = rows.get(0);
            long index = ((Number) row.get("last_included_index")).longValue();
            long term = ((Number) row.get("last_included_term")).longValue();
            return new SnapshotMeta(index, term);
        } catch (Exception e) {
            log.warn("raft.snapshot.load.failed nodeId={} err={}", nodeId, e.getMessage());
            return null;
        }
    }

    @Override
    public void saveSnapshot(String nodeId, long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
        try {
            jdbc.update(
                "INSERT INTO raft_snapshot(node_id, last_included_index, last_included_term, data) VALUES (?, ?, ?, ?) "
                    + "ON CONFLICT (node_id) DO UPDATE SET "
                    + "last_included_index = EXCLUDED.last_included_index, "
                    + "last_included_term = EXCLUDED.last_included_term, "
                    + "data = EXCLUDED.data, created_at = now()",
                nodeId, lastIncludedIndex, lastIncludedTerm, data
            );
        } catch (Exception e) {
            log.warn("raft.snapshot.save.failed nodeId={} index={} err={}", nodeId, lastIncludedIndex, e.getMessage());
        }
    }
}