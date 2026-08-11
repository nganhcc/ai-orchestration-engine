package com.nganhcc.orchestration.raftcore;

import com.nganhcc.orchestration.raftcore.RaftMessages.*;
import java.util.Map;

public final class InJvmRaftTransport implements RaftTransport {

    private final Map<String, RaftNode> nodesById;

    public InJvmRaftTransport(Map<String, RaftNode> nodesById) {
        this.nodesById = nodesById;
    }

    @Override
    public RequestVoteResponse sendRequestVote(String targetNodeId, RequestVoteRequest req) {
        RaftNode target = nodesById.get(targetNodeId);
        if (target == null) {
            // Coi như "unreachable" - trả về vote bị từ chối với term = 0
            // để caller không nhầm là mình lạc hậu.
            return new RequestVoteResponse(0, false);
        }
        return target.onReceiveRequestVote(req);
    }

    @Override
    public AppendEntriesResponse sendAppendEntries(String targetNodeId, AppendEntriesRequest req) {
        RaftNode target = nodesById.get(targetNodeId);
        if (target == null) {
            return new AppendEntriesResponse(0, false, 0);
        }
        return target.onReceiveAppendEntries(req);
    }

    @Override
    public InstallSnapshotResponse sendInstallSnapshot(String targetNodeId, InstallSnapshotRequest req) {
        RaftNode target = nodesById.get(targetNodeId);
        if (target == null) {
            return new InstallSnapshotResponse(0, false);
        }
        return target.onReceiveInstallSnapshot(req);
    }
}