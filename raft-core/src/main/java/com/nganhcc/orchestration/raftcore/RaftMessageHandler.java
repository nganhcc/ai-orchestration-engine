package com.nganhcc.orchestration.raftcore;

import com.nganhcc.orchestration.raftcore.RaftMessages.*;

public final class RaftMessageHandler {

    private RaftMessageHandler() {}

    public static RequestVoteResponse handleRequestVote(RaftState state, RequestVoteRequest req) {
        // 1. Term cũ hơn -> reject ngay, không đụng gì tới state
        if (req.term() < state.currentTerm) {
            return new RequestVoteResponse(state.currentTerm, false);
        }

        // 2. Term mới hơn -> phải cập nhật currentTerm và lùi về FOLLOWER trước khi xét tiếp,
        //    đồng thời votedFor phải reset vì đây là term mới, chưa vote ai cả
        if (req.term() > state.currentTerm) {
            state.currentTerm = req.term();
            state.votedFor = null;
            state.nodeState = NodeState.FOLLOWER;
        }

        // 3. Election restriction (Raft paper §5.4.1): chỉ vote cho candidate có log
        //    ÍT NHẤT MỚI BẰNG log của mình. "Mới hơn" nghĩa là: term của entry cuối cao hơn,
        //    hoặc term bằng nhau nhưng index cuối dài hơn/bằng.
        boolean candidateLogUpToDate =
            req.lastLogTerm() > state.log.lastTerm()
            || (req.lastLogTerm() == state.log.lastTerm() && req.lastLogIndex() >= state.log.lastIndex());

        // 4. Chỉ được vote nếu CHƯA vote ai ở term này (hoặc đã vote đúng candidate này -
        //    trường hợp request bị gửi lại) VÀ log candidate đủ mới
        boolean canVote = (state.votedFor == null || state.votedFor.equals(req.candidateId()))
            && candidateLogUpToDate;

        if (canVote) {
            state.votedFor = req.candidateId();
            return new RequestVoteResponse(state.currentTerm, true);
        }
        return new RequestVoteResponse(state.currentTerm, false);
    }

    public static AppendEntriesResponse handleAppendEntries(RaftState state, AppendEntriesRequest req) {
        // 1. Term cũ hơn -> reject ngay, log KHÔNG được đụng vào (đã giải thích lượt trước)
        if (req.term() < state.currentTerm) {
            return new AppendEntriesResponse(state.currentTerm, false, 0);
        }

        // 2. Term mới hơn hoặc bằng -> đây là leader hợp lệ (hiện tại hoặc mới), lùi về FOLLOWER.
        //    Quan trọng: dù term BẰNG currentTerm cũng phải lùi về FOLLOWER, vì 1 candidate
        //    đang tự ứng cử ở cùng term có thể nhận AppendEntries từ 1 leader đã thắng trước đó.
        if (req.term() >= state.currentTerm) {
            state.currentTerm = req.term();
            state.nodeState = NodeState.FOLLOWER;
        }

        // 3. Check log consistency tại prevLogIndex/prevLogTerm
        long termAtPrev = state.log.termAt(req.prevLogIndex());
        if (req.prevLogIndex() > 0 && termAtPrev != req.prevLogTerm()) {
            // không khớp (thiếu entry, hoặc entry khác term) -> reject,
            // leader sẽ tự giảm nextIndex và retry ở lần gửi sau
            return new AppendEntriesResponse(state.currentTerm, false, 0);
        }

        // 4. Log khớp -> ghi từng entry mới (appendOrOverwrite tự xử lý idempotent + conflict)
        for (LogEntry entry : req.entries()) {
            state.log.appendOrOverwrite(entry);
        }

        // 5. Cập nhật commitIndex theo leader, nhưng không được vượt quá log thực tế của mình
        long lastNewIndex = req.entries().isEmpty() ? req.prevLogIndex() : state.log.lastIndex();
        if (req.leaderCommit() > state.commitIndex) {
            state.commitIndex = Math.min(req.leaderCommit(), lastNewIndex);
        }

        return new AppendEntriesResponse(state.currentTerm, true, lastNewIndex);
    }
}