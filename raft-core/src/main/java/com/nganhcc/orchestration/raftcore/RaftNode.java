package com.nganhcc.orchestration.raftcore;

import com.nganhcc.orchestration.raftcore.RaftMessages.*;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Bọc quanh RaftState (1.1) + RaftMessageHandler (1.2) để thêm phần "chủ động theo thời gian":
 * election timeout, heartbeat, quy trình ứng cử. Chưa dùng network thật - giao tiếp giữa các
 * node qua RaftTransport (ở đây là InJvmRaftTransport, gọi hàm trực tiếp trong cùng JVM).
 */
public final class RaftNode {

    private final RaftState state;
    private final Set<String> peerIds;      // id của các node khác trong cluster, không gồm chính nó
    private final RaftTransport transport;
    private final RaftEventListener eventListener;
    private final ScheduledExecutorService scheduler;
    private final Random random = new Random();

    private ScheduledFuture<?> electionTask;
    private ScheduledFuture<?> heartbeatTask;
    private volatile boolean stopped = false;
    private long electionStartedAtNanos = 0L;
    private long currentElectionTimeoutMs = 0L;

    // Chỉ dùng khi node là LEADER
    private final java.util.Map<String, Long> nextIndex = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, Long> matchIndex = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int SNAPSHOT_THRESHOLD = 100; // Số entries tối đa trước khi trigger snapshot tự động

    public RaftNode(String selfId, Set<String> peerIds, RaftTransport transport) {
        this(selfId, peerIds, transport, RaftEventListener.noOp(), RaftLogStore.NO_OP);
    }

    public RaftNode(String selfId, Set<String> peerIds, RaftTransport transport, RaftEventListener eventListener) {
        this(selfId, peerIds, transport, eventListener, RaftLogStore.NO_OP);
    }

    public RaftNode(String selfId, Set<String> peerIds, RaftTransport transport, RaftEventListener eventListener, RaftLogStore store) {
        this.state = new RaftState(selfId, store);
        this.peerIds = peerIds;
        this.transport = transport;
        this.eventListener = eventListener == null ? RaftEventListener.noOp() : eventListener;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "raft-" + selfId));
    }

    public RaftState state() {
        return state;
    }

    public synchronized boolean isLeader() {
        return state.nodeState == NodeState.LEADER;
    }

    public synchronized long epoch() {
        return state.epoch;
    }

    public synchronized void triggerSnapshot() {
        long lastApplied = state.lastApplied;
        if (lastApplied <= state.lastSnapshotIndex) {
            return;
        }
        long term = state.log.termAt(lastApplied);
        // Tạo snapshot rỗng (chỉ lưu index/term phục vụ fencing và compaction)
        state.log.compactUpTo(lastApplied, term);
        state.lastSnapshotIndex = lastApplied;
        state.lastSnapshotTerm = term;
        eventListener.snapshotCreated(state.selfId, lastApplied, term);
    }

    public synchronized void checkAutoSnapshot() {
        if (state.log.size() >= SNAPSHOT_THRESHOLD) {
            triggerSnapshot();
        }
    }

    // ---------- Vòng đời ----------

    public synchronized void start() {
        stopped = false;
        resetElectionTimer();
    }

    public synchronized void stop() {
        stopped = true;
        if (electionTask != null) electionTask.cancel(false);
        if (heartbeatTask != null) heartbeatTask.cancel(false);
        scheduler.shutdownNow();
    }

    // ---------- Nhận RPC từ node khác (qua transport) ----------

    public synchronized RequestVoteResponse onReceiveRequestVote(RequestVoteRequest req) {
        long beforeTerm = state.currentTerm;
        long beforeEpoch = state.epoch;
        NodeState beforeState = state.nodeState;
        RequestVoteResponse resp = RaftMessageHandler.handleRequestVote(state, req);
        if (req.term() > beforeTerm && beforeState != NodeState.FOLLOWER && state.nodeState == NodeState.FOLLOWER) {
            notifyStepDown(req.candidateId(), beforeTerm, beforeEpoch, "request_vote", beforeState == NodeState.LEADER);
        }
        if (resp.voteGranted()) {
            // đã "cam kết" với candidate này trong 1 khoảng thời gian -> reset để không
            // tự ứng cử đè lên ngay sau khi vừa vote cho người khác
            resetElectionTimer();
        }
        return resp;
    }

    public synchronized AppendEntriesResponse onReceiveAppendEntries(AppendEntriesRequest req) {
        long beforeTerm = state.currentTerm;
        long beforeEpoch = state.epoch;
        NodeState beforeState = state.nodeState;
        AppendEntriesResponse resp = RaftMessageHandler.handleAppendEntries(state, req);
        if (state.nodeState == NodeState.FOLLOWER && beforeState != NodeState.FOLLOWER) {
            notifyStepDown(req.leaderId(), beforeTerm, beforeEpoch, "append_entries", beforeState == NodeState.LEADER);
        }
        if (resp.success()) {
            // AppendEntries hợp lệ từ 1 leader thật -> chứng tỏ cluster đang có leader,
            // reset để không tự timeout giữa chừng
            resetElectionTimer();
        }
        return resp;
    }

    public synchronized InstallSnapshotResponse onReceiveInstallSnapshot(InstallSnapshotRequest req) {
        long beforeTerm = state.currentTerm;
        long beforeEpoch = state.epoch;
        NodeState beforeState = state.nodeState;
        InstallSnapshotResponse resp = RaftMessageHandler.handleInstallSnapshot(state, req);
        if (state.nodeState == NodeState.FOLLOWER && beforeState != NodeState.FOLLOWER) {
            notifyStepDown(req.leaderId(), beforeTerm, beforeEpoch, "install_snapshot", beforeState == NodeState.LEADER);
        }
        if (resp.success()) {
            eventListener.installSnapshotReceived(state.selfId, req.leaderId(), req.lastIncludedIndex(), true);
            resetElectionTimer();
        } else {
            eventListener.installSnapshotReceived(state.selfId, req.leaderId(), req.lastIncludedIndex(), false);
        }
        return resp;
    }

    // ---------- Election timer ----------

    private void resetElectionTimer() {
        if (electionTask != null) {
            electionTask.cancel(false);
        }
        if (stopped) return;
        currentElectionTimeoutMs = 150 + random.nextInt(151); // random(150, 300)
        electionTask = scheduler.schedule(this::onElectionTimeout, currentElectionTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private void onElectionTimeout() {
        synchronized (this) {
            if (stopped) return;
            // Leader không tự bầu lại chính mình bằng election timer - chỉ follower/candidate mới timeout kiểu này
            if (state.nodeState == NodeState.LEADER) return;

            eventListener.electionTimeout(state.selfId, state.currentTerm, currentElectionTimeoutMs);
            electionStartedAtNanos = System.nanoTime();
            state.currentTerm += 1;
            state.nodeState = NodeState.CANDIDATE;
            state.votedFor = state.selfId;
            resetElectionTimer(); // cho vòng ứng cử này 1 timeout riêng, phòng split vote
        }
        runElection();
    }

    // ---------- Quy trình ứng cử ----------

    private void runElection() {
        long termAtStart;
        RequestVoteRequest req;
        synchronized (this) {
            termAtStart = state.currentTerm;
            req = new RequestVoteRequest(
                state.currentTerm, state.selfId, state.log.lastIndex(), state.log.lastTerm());
        }

        int votes = 1; // tự vote cho chính mình

        for (String peerId : peerIds) {
            long sendStartedAt = System.nanoTime();
            RequestVoteResponse resp;
            try {
                resp = transport.sendRequestVote(peerId, req);
            } catch (Exception e) {
                eventListener.voteResponse(
                    state.selfId,
                    peerId,
                    termAtStart,
                    millisSince(sendStartedAt),
                    false,
                    -1
                );
                continue; // coi như peer không phản hồi, bỏ qua phiếu này
            }

            synchronized (this) {
                if (state.currentTerm != termAtStart) {
                    // đã sang term khác từ lúc gửi request (ví dụ nhận AppendEntries mới hơn) -> dừng ngay
                    return;
                }
                eventListener.voteResponse(
                    state.selfId,
                    peerId,
                    termAtStart,
                    millisSince(sendStartedAt),
                    resp.voteGranted(),
                    resp.term()
                );
                if (resp.term() > state.currentTerm) {
                    long previousTerm = state.currentTerm;
                    long previousEpoch = state.epoch;
                    state.currentTerm = resp.term();
                    state.votedFor = null;
                    state.nodeState = NodeState.FOLLOWER;
                    state.epoch = 0;
                    notifyStepDown(peerId, previousTerm, previousEpoch, "higher_term_vote_response", false);
                    resetElectionTimer();
                    return;
                }
                if (resp.voteGranted()) {
                    votes++;
                }
            }
        }

        synchronized (this) {
            // phải check lại vẫn còn là CANDIDATE ở đúng term này trước khi tuyên bố thắng -
            // trong lúc chờ response, có thể đã bị step-down bởi 1 AppendEntries đến từ thread khác
            if (state.nodeState == NodeState.CANDIDATE
                && state.currentTerm == termAtStart
                && votes > totalClusterSize() / 2) {
                becomeLeader();
            }
        }
    }

    private int totalClusterSize() {
        return peerIds.size() + 1; // + chính nó
    }

    private void becomeLeader() {
        state.nodeState = NodeState.LEADER;
        state.epoch = state.currentTerm;
        long lastLogIndex = state.log.lastIndex();
        for (String peerId : peerIds) {
            nextIndex.put(peerId, lastLogIndex + 1);
            matchIndex.put(peerId, 0L);
        }
        if (electionTask != null) {
            electionTask.cancel(false); // leader không cần election timer nữa
        }
        long elapsedMs = electionStartedAtNanos == 0L ? -1L : millisSince(electionStartedAtNanos);
        electionStartedAtNanos = 0L;
        eventListener.leaderElected(state.selfId, state.currentTerm, state.epoch, elapsedMs);
        heartbeatTask = scheduler.scheduleAtFixedRate(
            this::sendHeartbeatToAll, 0, 50, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeatToAll() {
        long termAtStart;
        long lastSnapshotIdx;
        long lastSnapshotTrm;
        synchronized (this) {
            if (state.nodeState != NodeState.LEADER || stopped) {
                if (heartbeatTask != null) heartbeatTask.cancel(false);
                return;
            }
            termAtStart = state.currentTerm;
            lastSnapshotIdx = state.lastSnapshotIndex;
            lastSnapshotTrm = state.lastSnapshotTerm;
        }

        for (String peerId : peerIds) {
            long peerNextIdx = nextIndex.getOrDefault(peerId, 1L);
            if (peerNextIdx <= lastSnapshotIdx) {
                // Follower bị lag quá xa, đã bị compact log -> Gửi InstallSnapshot
                InstallSnapshotRequest snapshotReq = new InstallSnapshotRequest(
                    termAtStart,
                    state.selfId,
                    lastSnapshotIdx,
                    lastSnapshotTrm,
                    new byte[0] // Demo snapshot rỗng
                );
                eventListener.installSnapshotSent(state.selfId, peerId, lastSnapshotIdx);
                long sendStartedAt = System.nanoTime();
                InstallSnapshotResponse snapshotResp;
                try {
                    snapshotResp = transport.sendInstallSnapshot(peerId, snapshotReq);
                } catch (Exception e) {
                    continue;
                }
                synchronized (this) {
                    if (state.currentTerm != termAtStart) return;
                    if (snapshotResp.term() > state.currentTerm) {
                        long previousTerm = state.currentTerm;
                        long previousEpoch = state.epoch;
                        state.currentTerm = snapshotResp.term();
                        state.votedFor = null;
                        state.nodeState = NodeState.FOLLOWER;
                        state.epoch = 0;
                        notifyStepDown(peerId, previousTerm, previousEpoch, "higher_term_install_snapshot_response", true);
                        if (heartbeatTask != null) heartbeatTask.cancel(false);
                        resetElectionTimer();
                        return;
                    }
                    if (snapshotResp.success()) {
                        nextIndex.put(peerId, lastSnapshotIdx + 1);
                        matchIndex.put(peerId, lastSnapshotIdx);
                    }
                }
            } else {
                // Gửi AppendEntries thông thường
                AppendEntriesRequest req;
                synchronized (this) {
                    req = new AppendEntriesRequest(
                        state.currentTerm,
                        state.selfId,
                        state.log.lastIndex(),
                        state.log.lastTerm(),
                        List.of(),               // rỗng = heartbeat, chưa replicate gì ở bước này
                        state.commitIndex);
                }
                long sendStartedAt = System.nanoTime();
                AppendEntriesResponse resp;
                try {
                    resp = transport.sendAppendEntries(peerId, req);
                } catch (Exception e) {
                    eventListener.heartbeat(
                        state.selfId,
                        peerId,
                        termAtStart,
                        millisSince(sendStartedAt),
                        false,
                        -1
                    );
                    continue;
                }
                synchronized (this) {
                    if (state.currentTerm != termAtStart) return;
                    eventListener.heartbeat(
                        state.selfId,
                        peerId,
                        termAtStart,
                        millisSince(sendStartedAt),
                        resp.term() <= state.currentTerm,
                        resp.term()
                    );
                    if (resp.term() > state.currentTerm) {
                        long previousTerm = state.currentTerm;
                        long previousEpoch = state.epoch;
                        state.currentTerm = resp.term();
                        state.votedFor = null;
                        state.nodeState = NodeState.FOLLOWER;
                        state.epoch = 0;
                        notifyStepDown(peerId, previousTerm, previousEpoch, "higher_term_append_entries_response", true);
                        if (heartbeatTask != null) heartbeatTask.cancel(false);
                        resetElectionTimer();
                        return;
                    }
                    if (resp.success()) {
                        long nextIdx = req.prevLogIndex() + req.entries().size() + 1;
                        long matchIdx = req.prevLogIndex() + req.entries().size();
                        nextIndex.put(peerId, Math.max(nextIndex.getOrDefault(peerId, 1L), nextIdx));
                        matchIndex.put(peerId, Math.max(matchIndex.getOrDefault(peerId, 0L), matchIdx));
                    } else {
                        // Trừ phi term của response lớn hơn (đã xử lý ở trên), nếu fail do consistency, ta lùi nextIndex của peer đó
                        long currentNext = nextIndex.getOrDefault(peerId, 1L);
                        if (currentNext > 1) {
                            nextIndex.put(peerId, currentNext - 1);
                        }
                    }
                }
            }
        }
    }

    private void notifyStepDown(String sourceNodeId, long fromTerm, long fromEpoch, String reason, boolean cancelHeartbeat) {
        if (cancelHeartbeat && heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
        eventListener.stepDown(state.selfId, sourceNodeId, fromTerm, fromEpoch, state.currentTerm, reason);
    }

    private long millisSince(long startedAtNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
    }
}
