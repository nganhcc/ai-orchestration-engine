package com.nganhcc.orchestration.raftcore;

/**
 * Receives entries after they become committed locally. The command remains opaque to Raft;
 * callers may use this hook for a state machine, while the default implementation does nothing.
 */
@FunctionalInterface
public interface RaftCommitListener {

    void onCommitted(String nodeId, LogEntry entry);

    static RaftCommitListener noOp() {
        return (nodeId, entry) -> { };
    }
}
