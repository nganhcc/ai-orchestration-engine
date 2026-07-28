package com.nganhcc.orchestration.raftcore;

public final class RaftState {

    public final String selfId;
    public NodeState nodeState = NodeState.FOLLOWER;

    public long currentTerm = 0;
    public String votedFor = null;

    public final RaftLog log = new RaftLog();

    public long commitIndex = 0;
    public long lastApplied = 0;

    public RaftState(String selfId) {
        this.selfId = selfId;
    }
}