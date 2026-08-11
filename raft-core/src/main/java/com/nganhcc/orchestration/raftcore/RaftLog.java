package com.nganhcc.orchestration.raftcore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private long snapshotOffset = 0;
    private long snapshotOffsetTerm = 0;

    private final String nodeId;
    private final RaftLogStore store;

    public RaftLog() {
        this("unknown", RaftLogStore.NO_OP);
    }

    public RaftLog(String nodeId, RaftLogStore store) {
        this.nodeId = nodeId == null ? "unknown" : nodeId;
        this.store = store == null ? RaftLogStore.NO_OP : store;
    }

    /**
     * Khôi phục snapshotOffset/Term khi node restart từ store (PostgreSQL). Khác compactUpTo:
     * method này KHÔNG đụng entries (log đã bị compact ở store) và KHÔNG gọi store.compactUpTo
     * (snapshot đã tồn tại trong store, không cần ghi lại).
     */
    public synchronized void restoreSnapshotOffset(long lastIncludedIndex, long lastIncludedTerm) {
        if (lastIncludedIndex < 0) {
            throw new IllegalArgumentException("lastIncludedIndex phải >= 0");
        }
        this.snapshotOffset = lastIncludedIndex;
        this.snapshotOffsetTerm = lastIncludedTerm;
    }

    public synchronized void compactUpTo(long lastIncludedIndex, long lastIncludedTerm) {
        if (lastIncludedIndex < 0) {
            throw new IllegalArgumentException("lastIncludedIndex phải >= 0");
        }
        if (lastIncludedIndex <= snapshotOffset) {
            return; // Đã compact qua index này rồi
        }
        long listIdx = lastIncludedIndex - snapshotOffset - 1;
        if (listIdx >= entries.size()) {
            throw new IllegalArgumentException("Không thể compact vượt quá lastIndex: " + lastIndex());
        }
        int keepFrom = (int) (listIdx + 1);
        if (keepFrom < entries.size()) {
            entries.subList(0, keepFrom).clear();
        } else {
            entries.clear();
        }
        snapshotOffset = lastIncludedIndex;
        snapshotOffsetTerm = lastIncludedTerm;
        store.compactUpTo(nodeId, lastIncludedIndex, lastIncludedTerm);
    }

    public synchronized long getSnapshotOffset() {
        return snapshotOffset;
    }

    public synchronized long getSnapshotOffsetTerm() {
        return snapshotOffsetTerm;
    }

    public synchronized LogEntry appendNew(long term, byte[] command) {
        long newIndex = lastIndex() + 1;
        LogEntry entry = new LogEntry(term, newIndex, command);
        entries.add(entry);
        store.append(nodeId, entry);
        return entry;
    }

    public synchronized void appendOrOverwrite(LogEntry entry) {
        long listIdx = entry.index() - snapshotOffset - 1;
        if (listIdx < 0) {
            // Entry này đã bị compact, bỏ qua hoặc ném exception tuỳ thiết kế. Ở đây coi như đã có.
            return;
        }
        if (listIdx < entries.size()) {
            LogEntry existing = entries.get((int) listIdx);
            if (existing.term() == entry.term()) {
                return; // idempotent: entry cũ gửi lại, không làm gì
            }
            truncateFrom(entry.index()); // conflicting entry -> cắt rồi ghi đè
            entries.add(entry);
            store.append(nodeId, entry);
        } else if (listIdx == entries.size()) {
            entries.add(entry);
            store.append(nodeId, entry);
        } else {
            throw new IllegalStateException(
                "Gap trong log: cố ghi index=" + entry.index()
                    + " nhưng log hiện chỉ có tới index=" + lastIndex());
        }
    }

    public synchronized void truncateFrom(long fromIndex) {
        if (fromIndex < 1) {
            throw new IllegalArgumentException("fromIndex phải >= 1");
        }
        if (fromIndex <= snapshotOffset) {
            throw new IllegalArgumentException("Không thể truncate log đã bị compact up to index: " + snapshotOffset);
        }
        long listIdx = fromIndex - snapshotOffset - 1;
        int keepUntil = (int) Math.min(entries.size(), listIdx);
        if (keepUntil < entries.size()) {
            entries.subList(keepUntil, entries.size()).clear();
        }
        store.truncateFrom(nodeId, fromIndex);
    }

    public synchronized Optional<LogEntry> getEntry(long index) {
        long listIdx = index - snapshotOffset - 1;
        if (listIdx < 0 || listIdx >= entries.size()) {
            return Optional.empty();
        }
        return Optional.of(entries.get((int) listIdx));
    }

    public synchronized long termAt(long index) {
        if (index == 0) return 0;
        if (index == snapshotOffset) return snapshotOffsetTerm;
        if (index < snapshotOffset) return 0; // Đã bị compact
        return getEntry(index).map(LogEntry::term).orElse(0L);
    }

    public synchronized long lastIndex() {
        return entries.isEmpty() ? snapshotOffset : entries.get(entries.size() - 1).index();
    }

    public synchronized long lastTerm() {
        return entries.isEmpty() ? snapshotOffsetTerm : entries.get(entries.size() - 1).term();
    }

    public synchronized int size() {
        return entries.size();
    }
}