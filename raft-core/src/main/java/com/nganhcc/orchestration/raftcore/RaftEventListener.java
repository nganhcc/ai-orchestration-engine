package com.nganhcc.orchestration.raftcore;

public interface RaftEventListener {

    RaftEventListener NO_OP = new RaftEventListener() {};

    static RaftEventListener noOp() {
        return NO_OP;
    }

    default void electionTimeout(String nodeId, long term, long timeoutMs) {}

    default void voteResponse(
            String nodeId,
            String peerId,
            long term,
            long elapsedMs,
            boolean voteGranted,
            long responseTerm
    ) {}

    default void leaderElected(String nodeId, long term, long epoch, long elapsedMs) {}

    default void heartbeat(
            String nodeId,
            String peerId,
            long term,
            long elapsedMs,
            boolean success,
            long responseTerm
    ) {}

    default void stepDown(
            String nodeId,
            String sourceNodeId,
            long fromTerm,
            long fromEpoch,
            long toTerm,
            String reason
    ) {}
}
