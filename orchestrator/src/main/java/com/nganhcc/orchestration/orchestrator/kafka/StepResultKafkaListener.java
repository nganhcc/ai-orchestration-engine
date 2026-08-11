package com.nganhcc.orchestration.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nganhcc.orchestration.common.StepResultEvent;
import com.nganhcc.orchestration.orchestrator.service.StepService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class StepResultKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(StepResultKafkaListener.class);

    private final StepService stepService;
    private final ObjectMapper objectMapper;

    public StepResultKafkaListener(StepService stepService, ObjectMapper objectMapper) {
        this.stepService = stepService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "worker-step-result", groupId = "orchestrator-group")
    public void onStepResult(String message) {
        try {
            StepResultEvent event = objectMapper.readValue(message, StepResultEvent.class);
            log.info("Received StepResultEvent via Kafka: stepId={}, epoch={}", event.stepId(), event.epoch());

            UUID stepId = UUID.fromString(event.stepId());
            boolean applied = stepService.markStepDone(stepId, event.epoch(), event.resultJson());
            if (applied) {
                log.info("Successfully marked step {} as DONE", stepId);
            } else {
                log.warn("Rejected step result for step {} due to stale leader epoch {}", stepId, event.epoch());
            }
        } catch (Exception e) {
            log.error("Failed to process step result message: {}", message, e);
        }
    }
}
