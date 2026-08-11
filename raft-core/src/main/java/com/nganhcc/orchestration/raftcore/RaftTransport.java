package com.nganhcc.orchestration.raftcore;

import com.nganhcc.orchestration.raftcore.RaftMessages.*;

public interface RaftTransport {
    RequestVoteResponse sendRequestVote(String targetNodeId, RequestVoteRequest req);
    AppendEntriesResponse sendAppendEntries(String targetNodeId, AppendEntriesRequest req);
    InstallSnapshotResponse sendInstallSnapshot(String targetNodeId, InstallSnapshotRequest req);
}