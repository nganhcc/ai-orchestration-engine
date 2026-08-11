package com.nganhcc.orchestration.worker.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nganhcc.orchestration.common.AssignStepEvent;
import com.nganhcc.orchestration.common.StepResultEvent;
import com.nganhcc.orchestration.worker.service.WorkerStepService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class StepAssignKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(StepAssignKafkaListener.class);

    private final WorkerStepService workerStepService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AtomicLong highestEpochSeen = new AtomicLong(0);

    public StepAssignKafkaListener(WorkerStepService workerStepService,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper) {
        this.workerStepService = workerStepService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "orchestrator-assign-step", groupId = "worker-group")
    public void onAssignStep(String message) {
        try {
            AssignStepEvent event = objectMapper.readValue(message, AssignStepEvent.class);
            long epoch = event.epoch();
            long currentHighest = highestEpochSeen.get();

            if (epoch < currentHighest) {
                log.warn("Rejecting ASSIGN_STEP via Kafka due to stale leader epoch {} < {}", epoch, currentHighest);
                return; // Drop message
            }
            highestEpochSeen.accumulateAndGet(epoch, Math::max);

            UUID stepId = UUID.fromString(event.stepId());
            String jobId = event.batchId();
            String documentId = event.documentId();

            log.info("Received ASSIGN_STEP via Kafka: stepId={}, jobId={}, doc={}", stepId, jobId, documentId);

            try {
                String resultJson = workerStepService.processStep(stepId, jobId, documentId);

                StepResultEvent resultEvent = new StepResultEvent(
                        event.traceId(),
                        jobId,
                        event.stepId(),
                        resultJson,
                        epoch
                );

                String responseMessage = objectMapper.writeValueAsString(resultEvent);
                log.info("Sending StepResultEvent for stepId={} via Kafka", stepId);
                kafkaTemplate.send("worker-step-result", event.stepId(), responseMessage);

            } catch (com.nganhcc.orchestration.worker.service.CircuitBreaker.CircuitOpenException e) {
                log.warn("Circuit breaker is OPEN. Dropping stepId={} so it will be reaped. Msg: {}", stepId, e.getMessage());
            } catch (Exception e) {
                log.error("Failed to process stepId={} due to LLM call error: {}. Dropping so it will be reaped.", stepId, e.getMessage());
            }

        } catch (Exception e) {
            log.error("Failed to process step assignment message: {}", message, e);
        }
    }
}
