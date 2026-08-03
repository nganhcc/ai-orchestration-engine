package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.RaftEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LoggingRaftEventListener implements RaftEventListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingRaftEventListener.class);

    @Override
    public void electionTimeout(String nodeId, long term, long timeoutMs) {
        log.info("raft.event=election.timeout nodeId={} term={} timeoutMs={}", nodeId, term, timeoutMs);
    }

    @Override
    public void voteResponse(
            String nodeId,
            String peerId,
            long term,
            long elapsedMs,
            boolean voteGranted,
            long responseTerm
    ) {
        log.info(
                "raft.event=vote.response nodeId={} peerId={} term={} elapsedMs={} granted={} responseTerm={}",
                nodeId,
                peerId,
                term,
                elapsedMs,
                voteGranted,
                responseTerm);
    }

    @Override
    public void leaderElected(String nodeId, long term, long elapsedMs) {
        log.info("raft.event=leader.elected nodeId={} term={} elapsedMs={}", nodeId, term, elapsedMs);
    }

    @Override
    public void heartbeat(
            String nodeId,
            String peerId,
            long term,
            long elapsedMs,
            boolean success,
            long responseTerm
    ) {
        log.info(
                "raft.event=heartbeat nodeId={} peerId={} term={} elapsedMs={} success={} responseTerm={}",
                nodeId,
                peerId,
                term,
                elapsedMs,
                success,
                responseTerm);
    }

    @Override
    public void stepDown(
            String nodeId,
            String sourceNodeId,
            long fromTerm,
            long toTerm,
            String reason
    ) {
        log.info(
                "raft.event=step_down nodeId={} sourceNodeId={} fromTerm={} toTerm={} reason={}",
                nodeId,
                sourceNodeId,
                fromTerm,
                toTerm,
                reason);
    }
}
