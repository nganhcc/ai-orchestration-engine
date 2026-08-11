package com.nganhcc.orchestration.raftcore;

/**
 * Notification emitted exactly once per locally committed log entry. The listener must make
 * applying an entry idempotent because a node can restart and replay its committed log.
 */
@FunctionalInterface
public interface RaftCommitListener {

    void onCommitted(String nodeId, LogEntry entry);

    static RaftCommitListener noOp() {
        return (nodeId, entry) -> { };
    }
}
