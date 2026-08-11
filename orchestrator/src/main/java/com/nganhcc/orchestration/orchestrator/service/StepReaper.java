package com.nganhcc.orchestration.orchestrator.service;

import com.nganhcc.orchestration.orchestrator.dao.JobStepDao;
import com.nganhcc.orchestration.orchestrator.metrics.StepMetrics;
import com.nganhcc.orchestration.orchestrator.raft.RaftClusterBootstrap;
import com.nganhcc.orchestration.raftcore.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class StepReaper {

    private static final Logger log = LoggerFactory.getLogger(StepReaper.class);

    private final JobStepDao jobStepDao;
    private final StepMetrics stepMetrics;
    private final RaftClusterBootstrap raftClusterBootstrap;
    private final long stalenessThresholdSeconds;

    @Autowired
    public StepReaper(JobStepDao jobStepDao,
                      @Autowired(required = false) StepMetrics stepMetrics,
                      @Autowired(required = false) RaftClusterBootstrap raftClusterBootstrap,
                      @Value("${orchestrator.stepStalenessThresholdSeconds:30}") long stalenessThresholdSeconds) {
        this.jobStepDao = jobStepDao;
        this.stepMetrics = stepMetrics;
        this.raftClusterBootstrap = raftClusterBootstrap;
        this.stalenessThresholdSeconds = stalenessThresholdSeconds;
    }

    @Scheduled(fixedDelay = 5000)
    public void reapStaleSteps() {
        RaftNode raftNode = raftClusterBootstrap != null ? raftClusterBootstrap.raftNode() : null;
        if (raftNode != null && !raftNode.isLeader()) {
            return; // Only execute reaper on Raft Leader
        }

        int reaped = jobStepDao.reapStaleSteps(stalenessThresholdSeconds);
        if (reaped > 0) {
            log.warn("StepReaper: Reaped and reset {} stale steps back to PENDING", reaped);
            if (stepMetrics != null) {
                stepMetrics.incrementStepsReassigned(reaped);
            }
        }
    }
}
