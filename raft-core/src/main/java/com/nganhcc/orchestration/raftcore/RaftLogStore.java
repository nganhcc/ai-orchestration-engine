package com.nganhcc.orchestration.raftcore;

import java.util.List;

/**
 * Trừu tượng hoá nơi lưu trữ durable cho RaftLog (ví dụ PostgreSQL qua JDBC ở module orchestrator).
 * Mặc định NO_OP để giữ behavior in-memory cũ khi không có persistence (test, in-JVM demo).
 *
 * Toàn bộ method được gọi TỪ TRONG khối synchronized của RaftLog — implement phải an toàn đa luồng
 * và KHÔNG được gọi ngược lại RaftLog (tránh reentrant lock / deadlock).
 */
public interface RaftLogStore {

    /** Ghi 1 entry mới (append hoặc ghi đè sau khi truncate). Idempotent theo (nodeId, logIndex). */
    void append(String nodeId, LogEntry entry);

    /** Xoá mọi entry có logIndex >= fromIndex của node này. */
    void truncateFrom(String nodeId, long fromIndex);

    /** Xoá mọi entry đã compact (logIndex <= lastIncludedIndex) và lưu snapshot. */
    void compactUpTo(String nodeId, long lastIncludedIndex, long lastIncludedTerm);

    /** Nạp toàn bộ log còn lại (sau snapshotOffset) của node này, theo thứ tự index tăng dần. */
    List<LogEntry> load(String nodeId);

    /** Nạp snapshot meta (lastIncludedIndex/Term). Trả về null nếu chưa có snapshot. */
    SnapshotMeta loadSnapshot(String nodeId);

    /** Lưu (upsert) snapshot meta + data. */
    void saveSnapshot(String nodeId, long lastIncludedIndex, long lastIncludedTerm, byte[] data);

    /** Persist Raft hard state associated with this node. */
    default void saveHardState(String nodeId, HardState hardState) {
    }

    /** Load Raft hard state, or null when this node has never persisted one. */
    default HardState loadHardState(String nodeId) {
        return null;
    }

    /** Meta thông tin của 1 snapshot. */
    record SnapshotMeta(long lastIncludedIndex, long lastIncludedTerm) {}

    record HardState(long currentTerm, String votedFor, long commitIndex, long lastApplied) {}

    RaftLogStore NO_OP = new RaftLogStore() {
        @Override public void append(String nodeId, LogEntry entry) {}
        @Override public void truncateFrom(String nodeId, long fromIndex) {}
        @Override public void compactUpTo(String nodeId, long lastIncludedIndex, long lastIncludedTerm) {}
        @Override public List<LogEntry> load(String nodeId) { return List.of(); }
        @Override public SnapshotMeta loadSnapshot(String nodeId) { return null; }
        @Override public void saveSnapshot(String nodeId, long lastIncludedIndex, long lastIncludedTerm, byte[] data) {}
    };
}
