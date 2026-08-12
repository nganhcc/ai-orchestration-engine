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
    private final RaftCommitListener commitListener;
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
    private final Object proposalLock = new Object();
    private final Object replicationLock = new Object();
    private static final int SNAPSHOT_THRESHOLD = 100; // Số entries tối đa trước khi trigger snapshot tự động

    public RaftNode(String selfId, Set<String> peerIds, RaftTransport transport) {
        this(selfId, peerIds, transport, RaftEventListener.noOp(), RaftLogStore.NO_OP);
    }

    public RaftNode(String selfId, Set<String> peerIds, RaftTransport transport, RaftEventListener eventListener) {
        this(selfId, peerIds, transport, eventListener, RaftLogStore.NO_OP);
    }

    public RaftNode(String selfId, Set<String> peerIds, RaftTransport transport, RaftEventListener eventListener, RaftLogStore store) {
        this(selfId, peerIds, transport, eventListener, store, RaftCommitListener.noOp());
    }

    public RaftNode(
            String selfId,
            Set<String> peerIds,
            RaftTransport transport,
            RaftEventListener eventListener,
            RaftLogStore store,
            RaftCommitListener commitListener) {
        this.state = new RaftState(selfId, store);
        this.peerIds = peerIds;
        this.transport = transport;
        this.eventListener = eventListener == null ? RaftEventListener.noOp() : eventListener;
        this.commitListener = commitListener == null ? RaftCommitListener.noOp() : commitListener;
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

    /**
     * Append an opaque command to the leader log and synchronously replicate it to a quorum.
     * Raft does not interpret or apply the command; the default commit listener is a no-op.
     */
    public CommitResult propose(byte[] command) {
        synchronized (proposalLock) {
            LogEntry entry;
            synchronized (this) {
                if (state.nodeState != NodeState.LEADER) {
                    throw new NotLeaderException(state.selfId, state.currentTerm);
                }
                entry = state.log.appendNew(state.currentTerm, command);
            }

            for (String peerId : peerIds) {
                replicateFollower(peerId);
            }

            synchronized (this) {
                advanceCommitIndex();
                if (state.commitIndex < entry.index()) {
                    throw new IllegalStateException(
                            "Raft command index=" + entry.index() + " did not reach a quorum");
                }
                return new CommitResult(entry.index(), entry.term());
            }
        }
    }

    public record CommitResult(long logIndex, long term) { }

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
        state.persistHardState();
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
        state.persistHardState();
        applyCommittedEntries();
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
        state.persistHardState();
        applyCommittedEntries();
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
            state.persistHardState();
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
                    state.persistHardState();
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
                sendSnapshotToFollower(peerId, termAtStart, lastSnapshotIdx, lastSnapshotTrm);
            } else {
                replicateFollower(peerId);
            }
        }
    }

    private void sendSnapshotToFollower(String peerId, long term, long snapshotIndex, long snapshotTerm) {
        InstallSnapshotRequest request = new InstallSnapshotRequest(
                term, state.selfId, snapshotIndex, snapshotTerm, new byte[0]);
        eventListener.installSnapshotSent(state.selfId, peerId, snapshotIndex);
        InstallSnapshotResponse response;
        try {
            response = transport.sendInstallSnapshot(peerId, request);
        } catch (Exception ignored) {
            return;
        }
        synchronized (this) {
            if (state.currentTerm != term) {
                return;
            }
            if (response.term() > state.currentTerm) {
                long previousTerm = state.currentTerm;
                long previousEpoch = state.epoch;
                state.currentTerm = response.term();
                state.votedFor = null;
                state.nodeState = NodeState.FOLLOWER;
                state.epoch = 0;
                state.persistHardState();
                notifyStepDown(peerId, previousTerm, previousEpoch,
                        "higher_term_install_snapshot_response", true);
                resetElectionTimer();
                return;
            }
            if (response.success()) {
                nextIndex.put(peerId, snapshotIndex + 1);
                matchIndex.put(peerId, snapshotIndex);
            }
        }
    }

    /** Replicate the follower's next log entry batch, backing up on a consistency rejection. */
    private boolean replicateFollower(String peerId) {
        synchronized (replicationLock) {
            for (int attempt = 0; attempt < 64; attempt++) {
                AppendEntriesRequest request;
                long termAtStart;
                synchronized (this) {
                    if (state.nodeState != NodeState.LEADER || stopped) {
                        return false;
                    }
                    termAtStart = state.currentTerm;
                    long next = nextIndex.getOrDefault(peerId, state.log.lastIndex() + 1);
                    if (next <= state.log.getSnapshotOffset()) {
                        return false;
                    }
                    long prevIndex = next - 1;
                    request = new AppendEntriesRequest(
                            termAtStart,
                            state.selfId,
                            prevIndex,
                            state.log.termAt(prevIndex),
                            state.log.entriesFrom(next, 64),
                            state.commitIndex);
                }

                long sendStartedAt = System.nanoTime();
                AppendEntriesResponse response;
                try {
                    response = transport.sendAppendEntries(peerId, request);
                } catch (Exception e) {
                    eventListener.heartbeat(state.selfId, peerId, termAtStart,
                            millisSince(sendStartedAt), false, -1);
                    return false;
                }

                synchronized (this) {
                    eventListener.heartbeat(state.selfId, peerId, termAtStart,
                            millisSince(sendStartedAt), response.term() <= state.currentTerm, response.term());
                    if (state.currentTerm != termAtStart) {
                        return false;
                    }
                    if (response.term() > state.currentTerm) {
                        long previousTerm = state.currentTerm;
                        long previousEpoch = state.epoch;
                        state.currentTerm = response.term();
                        state.votedFor = null;
                        state.nodeState = NodeState.FOLLOWER;
                        state.epoch = 0;
                        state.persistHardState();
                        notifyStepDown(peerId, previousTerm, previousEpoch,
                                "higher_term_append_entries_response", true);
                        resetElectionTimer();
                        return false;
                    }
                    if (response.success()) {
                        long match = Math.max(request.prevLogIndex(), response.matchIndex());
                        matchIndex.put(peerId, Math.max(matchIndex.getOrDefault(peerId, 0L), match));
                        nextIndex.put(peerId, match + 1);
                        return true;
                    }
                    long currentNext = nextIndex.getOrDefault(peerId, 1L);
                    if (currentNext <= 1) {
                        return false;
                    }
                    nextIndex.put(peerId, currentNext - 1);
                }
            }
            return false;
        }
    }

    private void advanceCommitIndex() {
        long newCommit = state.commitIndex;
        long lastIndex = state.log.lastIndex();
        for (long index = state.commitIndex + 1; index <= lastIndex; index++) {
            if (state.log.termAt(index) != state.currentTerm) {
                continue;
            }
            int replicated = 1;
            for (String peerId : peerIds) {
                if (matchIndex.getOrDefault(peerId, 0L) >= index) {
                    replicated++;
                }
            }
            if (replicated > totalClusterSize() / 2) {
                newCommit = index;
            }
        }
        if (newCommit > state.commitIndex) {
            state.commitIndex = newCommit;
            state.persistHardState();
            applyCommittedEntries();
        }
    }

    private void applyCommittedEntries() {
        while (state.lastApplied < state.commitIndex) {
            long next = state.lastApplied + 1;
            LogEntry entry = state.log.getEntry(next).orElse(null);
            if (entry == null) {
                if (next <= state.lastSnapshotIndex) {
                    state.lastApplied = state.lastSnapshotIndex;
                    continue;
                }
                return;
            }
            commitListener.onCommitted(state.selfId, entry);
            state.lastApplied = next;
            state.persistHardState();
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
