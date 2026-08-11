package com.nganhcc.orchestration.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nganhcc.orchestration.common.AssignStepEvent;
import com.nganhcc.orchestration.orchestrator.dao.JobStepDao;
import com.nganhcc.orchestration.orchestrator.dao.JobStepDao.JobStepRecord;
import com.nganhcc.orchestration.orchestrator.raft.RaftClusterBootstrap;
import com.nganhcc.orchestration.raftcore.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class StepDispatcher {

    private static final Logger log = LoggerFactory.getLogger(StepDispatcher.class);

    private final JobStepDao jobStepDao;
    private final RaftClusterBootstrap raftClusterBootstrap;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public StepDispatcher(JobStepDao jobStepDao,
                          @Autowired(required = false) RaftClusterBootstrap raftClusterBootstrap,
                          KafkaTemplate<String, String> kafkaTemplate,
                          ObjectMapper objectMapper) {
        this.jobStepDao = jobStepDao;
        this.raftClusterBootstrap = raftClusterBootstrap;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    private final java.util.Map<UUID, Long> deficits = new java.util.concurrent.ConcurrentHashMap<>();

    @Scheduled(fixedDelay = 500)
    public void dispatchPendingSteps() {
        RaftNode raftNode = raftClusterBootstrap != null ? raftClusterBootstrap.raftNode() : null;
        if (raftNode != null && !raftNode.isLeader()) {
            return; // Only dispatch steps if current node is the Raft Leader
        }

        long epoch = (raftNode != null && raftNode.isLeader()) ? raftNode.epoch() : 1L;

        // Get active running batches with pending steps and priority
        java.util.List<JobStepDao.BatchPriorityPending> activeList = jobStepDao.findActiveBatchesWithPendingCount();
        if (activeList.isEmpty()) {
            return;
        }

        java.util.Map<UUID, PriorityScheduler.BatchInfo> activeBatches = new java.util.HashMap<>();
        for (JobStepDao.BatchPriorityPending b : activeList) {
            activeBatches.put(b.batchId(), new PriorityScheduler.BatchInfo(b.priority(), b.pendingCount()));
        }

        // Determine next batch dispatch order (limit 50)
        List<UUID> dispatchOrder = PriorityScheduler.nextDispatchOrder(activeBatches, deficits, 50);
        if (dispatchOrder.isEmpty()) {
            return;
        }

        // Fetch actual pending steps for these batches
        List<JobStepDao.JobStepRecord> pendingSteps = jobStepDao.findPendingStepsForBatches(dispatchOrder, dispatchOrder.size());
        if (pendingSteps.isEmpty()) {
            return;
        }

        // Map pending steps by batch for fast lookup during dispatch order iteration
        java.util.Map<UUID, java.util.List<JobStepDao.JobStepRecord>> stepsByBatch = new java.util.HashMap<>();
        for (JobStepDao.JobStepRecord step : pendingSteps) {
            stepsByBatch.computeIfAbsent(step.batchId(), k -> new java.util.ArrayList<>()).add(step);
        }

        for (UUID batchId : dispatchOrder) {
            java.util.List<JobStepDao.JobStepRecord> steps = stepsByBatch.get(batchId);
            if (steps == null || steps.isEmpty()) {
                continue;
            }

            JobStepDao.JobStepRecord step = steps.remove(0);

            String workerId = "worker-kafka";
            boolean assigned = jobStepDao.markStepAssigned(step.stepId(), epoch, workerId);
            if (!assigned) {
                continue;
            }

            UUID traceId = UUID.randomUUID();
            AssignStepEvent event = new AssignStepEvent(
                    traceId.toString(),
                    step.batchId().toString(),
                    step.stepId().toString(),
                    step.documentId(),
                    epoch
            );

            try {
                String message = objectMapper.writeValueAsString(event);
                log.info("Dispatching ASSIGN_STEP stepId={} batchId={} worker={} epoch={} via Kafka",
                        step.stepId(), step.batchId(), workerId, epoch);
                kafkaTemplate.send("orchestrator-assign-step", step.stepId().toString(), message);
            } catch (Exception e) {
                log.error("Failed to serialize and dispatch stepId={} via Kafka", step.stepId(), e);
            }
        }
    }
}
