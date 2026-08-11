package com.nganhcc.orchestration.raftcore;

/** Raised when a client attempts to propose a command through a follower. */
public final class NotLeaderException extends IllegalStateException {

    public NotLeaderException(String nodeId, long term) {
        super("Node " + nodeId + " is not the Raft leader at term " + term);
    }
}
