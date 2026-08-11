package com.nganhcc.orchestration.raftcore;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm tra RaftLog khi được gắn RaftLogStore giả lập (in-memory):
 * - append/truncate/compact phải được persist xuống store
 * - load phải khôi phục đúng log sau khi tạo RaftLog mới từ store
 */
class RaftLogStoreTest {

    /** Store giả lập in-memory, mô phỏng hành vi của PostgreSQL. */
    static final class InMemoryStore implements RaftLogStore {
        final Map<String, List<LogEntry>> logs = new ConcurrentHashMap<>();
        final Map<String, RaftLogStore.SnapshotMeta> snapshots = new ConcurrentHashMap<>();

        @Override
        public void append(String nodeId, LogEntry entry) {
            logs.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(entry);
        }

        @Override
        public void truncateFrom(String nodeId, long fromIndex) {
            List<LogEntry> list = logs.get(nodeId);
            if (list == null) return;
            list.removeIf(e -> e.index() >= fromIndex);
        }

        @Override
        public void compactUpTo(String nodeId, long lastIncludedIndex, long lastIncludedTerm) {
            List<LogEntry> list = logs.get(nodeId);
            if (list != null) {
                list.removeIf(e -> e.index() <= lastIncludedIndex);
            }
            snapshots.put(nodeId, new RaftLogStore.SnapshotMeta(lastIncludedIndex, lastIncludedTerm));
        }

        @Override
        public List<LogEntry> load(String nodeId) {
            List<LogEntry> list = logs.get(nodeId);
            return list == null ? List.of() : List.copyOf(list);
        }

        @Override
        public RaftLogStore.SnapshotMeta loadSnapshot(String nodeId) {
            return snapshots.get(nodeId);
        }

        @Override
        public void saveSnapshot(String nodeId, long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
            snapshots.put(nodeId, new RaftLogStore.SnapshotMeta(lastIncludedIndex, lastIncludedTerm));
        }
    }

    @Test
    void appendNew_persistsToStore() {
        InMemoryStore store = new InMemoryStore();
        RaftLog log = new RaftLog("node-1", store);

        log.appendNew(1, "cmd-1".getBytes());
        log.appendNew(1, "cmd-2".getBytes());

        List<LogEntry> persisted = store.load("node-1");
        assertEquals(2, persisted.size());
        assertEquals(1, persisted.get(0).index());
        assertEquals(2, persisted.get(1).index());
        assertArrayEquals("cmd-1".getBytes(), persisted.get(0).command());
    }

    @Test
    void appendOrOverwrite_conflict_truncatesAndPersists() {
        InMemoryStore store = new InMemoryStore();
        RaftLog log = new RaftLog("node-1", store);

        log.appendNew(1, "a".getBytes());
        log.appendNew(1, "b".getBytes());
        log.appendNew(1, "c".getBytes());

        // Conflict tại index=2 với term khác -> truncate từ 2, ghi đè
        log.appendOrOverwrite(new LogEntry(2, 2, "B2".getBytes()));

        List<LogEntry> persisted = store.load("node-1");
        assertEquals(2, persisted.size());
        assertEquals(1, persisted.get(0).index());
        assertEquals(2, persisted.get(1).index());
        assertEquals(2, persisted.get(1).term());
        assertArrayEquals("B2".getBytes(), persisted.get(1).command());
    }

    @Test
    void truncateFrom_persistsToStore() {
        InMemoryStore store = new InMemoryStore();
        RaftLog log = new RaftLog("node-1", store);

        log.appendNew(1, "a".getBytes());
        log.appendNew(1, "b".getBytes());
        log.appendNew(1, "c".getBytes());

        log.truncateFrom(2);

        List<LogEntry> persisted = store.load("node-1");
        assertEquals(1, persisted.size());
        assertEquals(1, persisted.get(0).index());
    }

    @Test
    void compactUpTo_persistsSnapshotAndTrimsLog() {
        InMemoryStore store = new InMemoryStore();
        RaftLog log = new RaftLog("node-1", store);

        log.appendNew(1, "a".getBytes());
        log.appendNew(1, "b".getBytes());
        log.appendNew(1, "c".getBytes());

        log.compactUpTo(2, 1);

        RaftLogStore.SnapshotMeta snapshot = store.loadSnapshot("node-1");
        assertNotNull(snapshot);
        assertEquals(2, snapshot.lastIncludedIndex());
        assertEquals(1, snapshot.lastIncludedTerm());

        List<LogEntry> persisted = store.load("node-1");
        assertEquals(1, persisted.size());
        assertEquals(3, persisted.get(0).index());
    }

    @Test
    void load_restoresLogIntoNewRaftLog() {
        InMemoryStore store = new InMemoryStore();
        RaftLog original = new RaftLog("node-1", store);
        original.appendNew(1, "a".getBytes());
        original.appendNew(2, "b".getBytes());
        original.appendNew(2, "c".getBytes());

        // Mô phỏng restart: tạo RaftLog mới từ cùng store
        RaftLog restored = new RaftLog("node-1", store);
        for (LogEntry entry : store.load("node-1")) {
            restored.appendOrOverwrite(entry);
        }

        assertEquals(3, restored.lastIndex());
        assertEquals(2, restored.lastTerm());
        assertArrayEquals("b".getBytes(), restored.getEntry(2).orElseThrow().command());
    }

    @Test
    void load_afterCompact_restoresSnapshotOffset() {
        InMemoryStore store = new InMemoryStore();
        RaftLog original = new RaftLog("node-1", store);
        original.appendNew(1, "a".getBytes());
        original.appendNew(1, "b".getBytes());
        original.appendNew(1, "c".getBytes());
        original.compactUpTo(2, 1);

        // Mô phỏng restart
        RaftLog restored = new RaftLog("node-1", store);
        RaftLogStore.SnapshotMeta snapshot = store.loadSnapshot("node-1");
        if (snapshot != null) {
            restored.restoreSnapshotOffset(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
        }
        for (LogEntry entry : store.load("node-1")) {
            restored.appendOrOverwrite(entry);
        }

        assertEquals(2, restored.getSnapshotOffset());
        assertEquals(3, restored.lastIndex());
        assertEquals(1, restored.termAt(2)); // snapshotOffsetTerm
        assertEquals(1, restored.termAt(3));
    }

    @Test
    void noOpStore_keepsInMemoryBehavior() {
        RaftLog log = new RaftLog(); // mặc định NO_OP
        log.appendNew(1, "a".getBytes());
        log.appendNew(1, "b".getBytes());
        assertEquals(2, log.lastIndex());
        assertEquals(1, log.termAt(1));
    }
}