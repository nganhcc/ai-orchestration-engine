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
}