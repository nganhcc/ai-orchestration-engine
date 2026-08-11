package com.nganhcc.orchestration.raftcore;

public final class RaftState {

    public final String selfId;
    public NodeState nodeState = NodeState.FOLLOWER;

    public long currentTerm = 0;
    public long epoch = 0;
    public String votedFor = null;

    public final RaftLog log;

    public long commitIndex = 0;
    public long lastApplied = 0;
    public long lastSnapshotIndex = 0;
    public long lastSnapshotTerm = 0;

    public RaftState(String selfId) {
        this(selfId, RaftLogStore.NO_OP);
    }

    public RaftState(String selfId, RaftLogStore store) {
        this.selfId = selfId;
        this.log = new RaftLog(selfId, store);
    }
}