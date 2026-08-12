package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.LogEntry;
import com.nganhcc.orchestration.raftcore.RaftLogStore;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Kiểm tra JdbcRaftLogStore bằng mock JdbcTemplate — verify SQL đúng, không cần DB thật.
 */
class JdbcRaftLogStoreTest {

    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final JdbcRaftLogStore store = new JdbcRaftLogStore(jdbc);

    @Test
    void append_usesUpsertSql() {
        LogEntry entry = new LogEntry(3, 5, "cmd".getBytes());
        store.append("node-a", entry);

        verify(jdbc).update(
            contains("INSERT INTO raft_log"),
            eq("node-a"), eq(5L), eq(3L), eq("cmd".getBytes())
        );
    }

    @Test
    void truncateFrom_usesDeleteSql() {
        store.truncateFrom("node-a", 4);

        verify(jdbc).update(
            contains("DELETE FROM raft_log"),
            eq("node-a"), eq(4L)
        );
    }

    @Test
    void compactUpTo_deletesAndSavesSnapshot() {
        store.compactUpTo("node-a", 10, 2);

        verify(jdbc).update(contains("DELETE FROM raft_log"), eq("node-a"), eq(10L));
        verify(jdbc).update(
            contains("INSERT INTO raft_snapshot"),
            eq("node-a"), eq(10L), eq(2L), any(byte[].class)
        );
    }

    @Test
    void load_parsesRowsIntoLogEntries() {
        when(jdbc.queryForList(anyString(), eq("node-a")))
            .thenReturn(List.of(
                Map.of("log_index", 1L, "term", 1L, "command", new byte[]{1}),
                Map.of("log_index", 2L, "term", 2L, "command", new byte[]{2})
            ));

        List<LogEntry> entries = store.load("node-a");

        assertEquals(2, entries.size());
        assertEquals(1, entries.get(0).index());
        assertEquals(1, entries.get(0).term());
        assertEquals(2, entries.get(1).index());
        assertEquals(2, entries.get(1).term());
        assertArrayEquals(new byte[]{1}, entries.get(0).command());
    }

    @Test
    void loadSnapshot_returnsNullWhenEmpty() {
        when(jdbc.queryForList(anyString(), eq("node-a"))).thenReturn(List.of());

        assertNull(store.loadSnapshot("node-a"));
    }

    @Test
    void loadSnapshot_parsesMeta() {
        when(jdbc.queryForList(anyString(), eq("node-a")))
            .thenReturn(List.of(Map.of("last_included_index", 10L, "last_included_term", 2L)));

        RaftLogStore.SnapshotMeta meta = store.loadSnapshot("node-a");

        assertNotNull(meta);
        assertEquals(10, meta.lastIncludedIndex());
        assertEquals(2, meta.lastIncludedTerm());
    }

    @Test
    void saveSnapshot_usesUpsertSql() {
        store.saveSnapshot("node-a", 10, 2, new byte[0]);

        verify(jdbc).update(
            contains("INSERT INTO raft_snapshot"),
            eq("node-a"), eq(10L), eq(2L), any(byte[].class)
        );
    }

    @Test
    void append_swallowsException() {
        doThrow(new RuntimeException("db down")).when(jdbc).update(anyString(), any(), any(), any(), any());
        assertDoesNotThrow(() -> store.append("node-a", new LogEntry(1, 1, new byte[0])));
    }

    @Test
    void saveHardState_usesUpsertSql() {
        store.saveHardState("node-a", new RaftLogStore.HardState(4, "node-b", 8, 7));

        verify(jdbc).update(
            contains("INSERT INTO raft_meta"),
            eq("node-a"), eq(4L), eq("node-b"), eq(8L), eq(7L)
        );
    }

    @Test
    void loadHardState_parsesRow() {
        when(jdbc.queryForList(anyString(), eq("node-a")))
            .thenReturn(List.of(Map.of(
                "current_term", 4L,
                "voted_for", "node-b",
                "commit_index", 8L,
                "last_applied", 7L
            )));

        assertEquals(new RaftLogStore.HardState(4, "node-b", 8, 7),
                store.loadHardState("node-a"));
    }
}
