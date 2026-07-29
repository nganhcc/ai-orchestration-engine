package com.nganhcc.orchestration.raftcore;

import com.nganhcc.orchestration.raftcore.RaftMessages.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RaftMessageHandlerTest {

    // ================= handleRequestVote =================

    @Test
    void requestVote_termCuHonThiReject_khongMutateState() {
        RaftState state = new RaftState("A");
        state.currentTerm = 5;
        state.votedFor = null;

        RequestVoteRequest req = new RequestVoteRequest(3, "B", 0, 0);
        RequestVoteResponse resp = RaftMessageHandler.handleRequestVote(state, req);

        assertFalse(resp.voteGranted());
        assertEquals(5, resp.term());
        // state không được đổi gì cả
        assertEquals(5, state.currentTerm);
        assertNull(state.votedFor);
    }

    @Test
    void requestVote_termMoiHonThiCapNhatCurrentTermVaLuiVeFollower() {
        RaftState state = new RaftState("A");
        state.currentTerm = 3;
        state.nodeState = NodeState.CANDIDATE;
        state.votedFor = "A"; // đang tự vote cho chính mình ở term cũ

        RequestVoteRequest req = new RequestVoteRequest(5, "B", 0, 0);
        RaftMessageHandler.handleRequestVote(state, req);

        assertEquals(5, state.currentTerm);
        assertEquals(NodeState.FOLLOWER, state.nodeState);
    }

    @Test
    void requestVote_logCandidateNganHonThiReject() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        // log của mình dài hơn candidate: lastIndex=3, lastTerm=1
        state.log.appendOrOverwrite(new LogEntry(1, 1, "x".getBytes()));
        state.log.appendOrOverwrite(new LogEntry(1, 2, "x".getBytes()));
        state.log.appendOrOverwrite(new LogEntry(1, 3, "x".getBytes()));

        // candidate có log ngắn hơn: lastIndex=1
        RequestVoteRequest req = new RequestVoteRequest(1, "B", 1, 1);
        RequestVoteResponse resp = RaftMessageHandler.handleRequestVote(state, req);

        assertFalse(resp.voteGranted(), "Log candidate ngắn hơn thì phải bị reject theo election restriction");
        assertNull(state.votedFor);
    }

    @Test
    void requestVote_logCandidateTermCuHonThiRejectDuChiSoDaiHon() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        // log mình: lastTerm=2
        state.log.appendOrOverwrite(new LogEntry(1, 1, "x".getBytes()));
        state.log.appendOrOverwrite(new LogEntry(2, 2, "x".getBytes()));

        // candidate: log dài hơn (index=5) nhưng lastTerm=1, cũ hơn về mặt lịch sử lãnh đạo
        RequestVoteRequest req = new RequestVoteRequest(1, "B", 5, 1);
        RequestVoteResponse resp = RaftMessageHandler.handleRequestVote(state, req);

        assertFalse(resp.voteGranted(), "Term entry cuối thấp hơn phải reject dù index dài hơn");
    }

    @Test
    void requestVote_logCandidateMoiBangThiGrant() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        state.log.appendOrOverwrite(new LogEntry(1, 1, "x".getBytes()));

        // candidate có log giống hệt (cùng lastTerm, cùng lastIndex)
        RequestVoteRequest req = new RequestVoteRequest(1, "B", 1, 1);
        RequestVoteResponse resp = RaftMessageHandler.handleRequestVote(state, req);

        assertTrue(resp.voteGranted());
        assertEquals("B", state.votedFor);
    }

    @Test
    void requestVote_daVoteChoNguoiKhacRoiThiRejectUngVienThu2() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        state.votedFor = "B"; // đã vote cho B ở term này

        RequestVoteRequest req = new RequestVoteRequest(1, "C", 0, 0);
        RequestVoteResponse resp = RaftMessageHandler.handleRequestVote(state, req);

        assertFalse(resp.voteGranted(), "Đã vote cho B rồi thì không được vote cho C ở cùng term");
        assertEquals("B", state.votedFor, "votedFor không được đổi");
    }

    @Test
    void requestVote_guiLaiRequestCuaCungCandidateDaVoteThiVanGrant_idempotent() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        state.votedFor = "B"; // đã vote cho B trước đó

        // B gửi lại đúng request cũ (do mất response trên đường mạng)
        RequestVoteRequest req = new RequestVoteRequest(1, "B", 0, 0);
        RequestVoteResponse resp = RaftMessageHandler.handleRequestVote(state, req);

        assertTrue(resp.voteGranted(), "Gửi lại request của đúng candidate đã vote thì vẫn phải grant");
    }

    // ================= handleAppendEntries =================

    @Test
    void appendEntries_termCuHonThiReject_logKhongDoi() {
        RaftState state = new RaftState("A");
        state.currentTerm = 5;
        state.log.appendOrOverwrite(new LogEntry(4, 1, "old".getBytes()));

        AppendEntriesRequest req = new AppendEntriesRequest(
            3, "leaderCu", 1, 4, List.of(new LogEntry(3, 2, "hack".getBytes())), 0);

        AppendEntriesResponse resp = RaftMessageHandler.handleAppendEntries(state, req);

        assertFalse(resp.success());
        assertEquals(5, resp.term());
        assertEquals(1, state.log.lastIndex(), "Log không được đụng vào khi term cũ hơn");
    }

    @Test
    void appendEntries_termBangNhauVanPhaiLuiCandidateVeFollower() {
        RaftState state = new RaftState("A");
        state.currentTerm = 5;
        state.nodeState = NodeState.CANDIDATE; // đang tự ứng cử ở đúng term 5

        AppendEntriesRequest req = new AppendEntriesRequest(
            5, "leaderThangCu", 0, 0, List.of(), 0);

        RaftMessageHandler.handleAppendEntries(state, req);

        assertEquals(NodeState.FOLLOWER, state.nodeState,
            "Term bằng nhau cũng phải lùi về FOLLOWER vì đã có leader thắng cử ở term này");
    }

    @Test
    void appendEntries_prevLogIndexKhongTonTaiThiReject() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        state.log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes())); // chỉ có tới index=1

        // leader nói prevLogIndex=4, nhưng follower chưa có tới đó
        AppendEntriesRequest req = new AppendEntriesRequest(
            1, "leader", 4, 2, List.of(new LogEntry(2, 5, "E".getBytes())), 0);

        AppendEntriesResponse resp = RaftMessageHandler.handleAppendEntries(state, req);

        assertFalse(resp.success());
        assertEquals(1, state.log.lastIndex(), "Log không được ghi gì khi prevLogIndex không khớp");
    }

    @Test
    void appendEntries_prevLogTermKhacThiReject() {
        RaftState state = new RaftState("A");
        state.currentTerm = 2;
        state.log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes())); // index=4 có term=1
        state.log.appendOrOverwrite(new LogEntry(1, 2, "B".getBytes()));
        state.log.appendOrOverwrite(new LogEntry(1, 3, "C".getBytes()));
        state.log.appendOrOverwrite(new LogEntry(1, 4, "D".getBytes()));

        // leader nói prevLogTerm tại index=4 phải là 2, nhưng follower đang có term=1 ở đó
        AppendEntriesRequest req = new AppendEntriesRequest(
            2, "leader", 4, 2, List.of(new LogEntry(2, 5, "E".getBytes())), 0);

        AppendEntriesResponse resp = RaftMessageHandler.handleAppendEntries(state, req);

        assertFalse(resp.success(), "prevLogTerm không khớp phải reject");
        assertEquals(4, state.log.lastIndex(), "Log không được ghi gì thêm khi bị reject");
    }

    @Test
    void appendEntries_conflictingEntryThiTruncateRoiGhiDe() {
        RaftState state = new RaftState("A");
        state.currentTerm = 2;
        state.log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes()));
        state.log.appendOrOverwrite(new LogEntry(1, 2, "B".getBytes()));
        state.log.appendOrOverwrite(new LogEntry(1, 3, "rac_tu_leader_cu".getBytes()));

        // leader hiện tại (term=2) gửi entry mới đè lên index=3, prevLogIndex=2 khớp (term=1)
        LogEntry newEntry = new LogEntry(2, 3, "C_dung".getBytes());
        AppendEntriesRequest req = new AppendEntriesRequest(
            2, "leader", 2, 1, List.of(newEntry), 0);

        AppendEntriesResponse resp = RaftMessageHandler.handleAppendEntries(state, req);

        assertTrue(resp.success());
        assertEquals(newEntry, state.log.getEntry(3).get());
        assertEquals(3, state.log.lastIndex());
    }

    @Test
    void appendEntries_heartbeatRongVanCapNhatCommitIndex() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        state.log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes()));
        state.log.appendOrOverwrite(new LogEntry(1, 2, "B".getBytes()));
        state.commitIndex = 0;

        // heartbeat rỗng, leaderCommit=2
        AppendEntriesRequest req = new AppendEntriesRequest(
            1, "leader", 2, 1, List.of(), 2);

        AppendEntriesResponse resp = RaftMessageHandler.handleAppendEntries(state, req);

        assertTrue(resp.success());
        assertEquals(2, state.commitIndex);
        assertEquals(2, resp.matchIndex());
    }

    @Test
    void appendEntries_commitIndexKhongDuocVuotQuaLogThucCo() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        state.log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes()));
        // follower chỉ mới có tới index=1

        // leader biết đúng follower đã khớp tới index=1 (prevLogIndex=1, prevLogTerm=1),
        // nhưng leaderCommit nói cao hơn nhiều vì leader đã replicate xong ở các follower khác
        AppendEntriesRequest req = new AppendEntriesRequest(
            1, "leader", 1, 1, List.of(), 10);

        RaftMessageHandler.handleAppendEntries(state, req);

        assertEquals(1, state.commitIndex,
            "commitIndex không được vượt quá log thực có của follower, dù leader nói cao hơn");
    }

    @Test
    void appendEntries_guiLaiEntryCuLaIdempotent() {
        RaftState state = new RaftState("A");
        state.currentTerm = 1;
        LogEntry entry = new LogEntry(1, 1, "A".getBytes());
        state.log.appendOrOverwrite(entry);

        // leader gửi lại đúng entry đã có (retry do mất ACK)
        AppendEntriesRequest req = new AppendEntriesRequest(
            1, "leader", 0, 0, List.of(entry), 0);

        AppendEntriesResponse resp = RaftMessageHandler.handleAppendEntries(state, req);

        assertTrue(resp.success());
        assertEquals(1, state.log.size(), "Gửi lại entry cũ không được tạo thêm bản ghi");
    }
}