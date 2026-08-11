package com.nganhcc.orchestration.raftcore;

import com.nganhcc.orchestration.raftcore.RaftMessages.InstallSnapshotRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.InstallSnapshotResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RaftSnapshotTest {

    @Test
    void testHandleInstallSnapshot() {
        RaftState state = new RaftState("follower");
        state.log.appendNew(1, "cmd1".getBytes());
        state.log.appendNew(1, "cmd2".getBytes());
        state.commitIndex = 1;
        state.lastApplied = 1;

        // 1. Request term cũ hơn -> reject
        state.currentTerm = 2;
        InstallSnapshotRequest reqOldTerm = new InstallSnapshotRequest(1, "leader", 2L, 1L, new byte[0]);
        InstallSnapshotResponse respOld = RaftMessageHandler.handleInstallSnapshot(state, reqOldTerm);
        assertFalse(respOld.success());
        assertEquals(2, respOld.term());

        // 2. Request term mới hơn -> chuyển follower, accept
        InstallSnapshotRequest reqNew = new InstallSnapshotRequest(3, "leader", 2L, 1L, new byte[0]);
        InstallSnapshotResponse respNew = RaftMessageHandler.handleInstallSnapshot(state, reqNew);
        assertTrue(respNew.success());
        assertEquals(3, state.currentTerm);
        assertEquals(NodeState.FOLLOWER, state.nodeState);
        assertEquals(2L, state.lastSnapshotIndex);
        assertEquals(1L, state.lastSnapshotTerm);
        assertEquals(2L, state.commitIndex);
        assertEquals(2L, state.lastApplied);
    }
}
