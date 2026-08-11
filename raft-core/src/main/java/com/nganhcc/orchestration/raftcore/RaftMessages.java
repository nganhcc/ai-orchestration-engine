package com.nganhcc.orchestration.raftcore;

import java.util.List;

public final class RaftMessages {

    private RaftMessages() {}

    public record RequestVoteRequest(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
    ) {}

    public record RequestVoteResponse(
        long term,
        boolean voteGranted
    ) {}

    public record AppendEntriesRequest(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,   // rỗng = heartbeat
        long leaderCommit
    ) {}

    public record AppendEntriesResponse(
        long term,
        boolean success,
        long matchIndex
    ) {}

    public record InstallSnapshotRequest(
        long term,
        String leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        byte[] data
    ) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InstallSnapshotRequest that = (InstallSnapshotRequest) o;
            if (term != that.term) return false;
            if (lastIncludedIndex != that.lastIncludedIndex) return false;
            if (lastIncludedTerm != that.lastIncludedTerm) return false;
            if (!leaderId.equals(that.leaderId)) return false;
            return java.util.Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            int result = (int) (term ^ (term >>> 32));
            result = 31 * result + leaderId.hashCode();
            result = 31 * result + (int) (lastIncludedIndex ^ (lastIncludedIndex >>> 32));
            result = 31 * result + (int) (lastIncludedTerm ^ (lastIncludedTerm >>> 32));
            result = 31 * result + java.util.Arrays.hashCode(data);
            return result;
        }
    }

    public record InstallSnapshotResponse(
        long term,
        boolean success
    ) {}
}