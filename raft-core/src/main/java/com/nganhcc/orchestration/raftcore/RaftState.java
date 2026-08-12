package com.nganhcc.orchestration.raftcore;

public final class RaftState {

    public final String selfId;
    public NodeState nodeState = NodeState.FOLLOWER;

    public long currentTerm = 0;
    public long epoch = 0;
    public String votedFor = null;

    public final RaftLog log;
    private final RaftLogStore store;

    public long commitIndex = 0;
    public long lastApplied = 0;
    public long lastSnapshotIndex = 0;
    public long lastSnapshotTerm = 0;

    public RaftState(String selfId) {
        this(selfId, RaftLogStore.NO_OP);
    }

    public RaftState(String selfId, RaftLogStore store) {
        this.selfId = selfId;
        this.store = store == null ? RaftLogStore.NO_OP : store;
        this.log = new RaftLog(selfId, this.store);
    }

    public void persistHardState() {
        store.saveHardState(selfId, new RaftLogStore.HardState(currentTerm, votedFor, commitIndex, lastApplied));
    }
}
