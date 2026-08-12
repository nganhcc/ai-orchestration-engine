package com.nganhcc.orchestration.raftcore;

public final class NotLeaderException extends IllegalStateException {

    public NotLeaderException(String nodeId, long term) {
        super("Node " + nodeId + " is not the Raft leader at term " + term);
    }
}
