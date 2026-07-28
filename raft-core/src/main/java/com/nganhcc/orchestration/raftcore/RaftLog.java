package com.nganhcc.orchestration.raftcore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();

    public synchronized LogEntry appendNew(long term, byte[] command) {
        long newIndex = lastIndex() + 1;
        LogEntry entry = new LogEntry(term, newIndex, command);
        entries.add(entry);
        return entry;
    }

    public synchronized void appendOrOverwrite(LogEntry entry) {
        long listIdx = entry.index() - 1;
        if (listIdx < 0) {
            throw new IllegalArgumentException("index phải >= 1");
        }
        if (listIdx < entries.size()) {
            LogEntry existing = entries.get((int) listIdx);
            if (existing.term() == entry.term()) {
                return; // idempotent: entry cũ gửi lại, không làm gì
            }
            truncateFrom(entry.index()); // conflicting entry -> cắt rồi ghi đè
            entries.add(entry);
        } else if (listIdx == entries.size()) {
            entries.add(entry);
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
        int keepUntil = (int) Math.min(entries.size(), fromIndex - 1);
        if (keepUntil < entries.size()) {
            entries.subList(keepUntil, entries.size()).clear();
        }
    }

    public synchronized Optional<LogEntry> getEntry(long index) {
        long listIdx = index - 1;
        if (listIdx < 0 || listIdx >= entries.size()) {
            return Optional.empty();
        }
        return Optional.of(entries.get((int) listIdx));
    }

    public synchronized long termAt(long index) {
        if (index == 0) return 0;
        return getEntry(index).map(LogEntry::term).orElse(0L);
    }

    public synchronized long lastIndex() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).index();
    }

    public synchronized long lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    public synchronized int size() {
        return entries.size();
    }
}