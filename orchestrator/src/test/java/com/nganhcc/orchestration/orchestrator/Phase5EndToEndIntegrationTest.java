package com.nganhcc.orchestration.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nganhcc.orchestration.common.AssignStepEvent;
import com.nganhcc.orchestration.common.StepResultEvent;
import com.nganhcc.orchestration.orchestrator.dao.JobStepDao;
import com.nganhcc.orchestration.orchestrator.dao.JobStepDao.JobStepRecord;
import com.nganhcc.orchestration.orchestrator.kafka.StepResultKafkaListener;
import com.nganhcc.orchestration.orchestrator.service.StepDispatcher;
import com.nganhcc.orchestration.orchestrator.service.StepService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class Phase5EndToEndIntegrationTest {

    private JobStepDao jobStepDao;
    private StepService stepService;
    private JdbcOperations jdbc;
    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper objectMapper;

    private final Map<UUID, String> stepStatusMap = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> outboxList = new ArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        jobStepDao = mock(JobStepDao.class);
        jdbc = mock(JdbcOperations.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        objectMapper = new ObjectMapper();

        stepService = new StepService(jobStepDao, jdbc);
    }

    @Test
    void testEndToEndPipeline_DispatchExecutionOutbox() throws Exception {
        UUID batchId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();

        stepStatusMap.put(stepId, "PENDING");
        JobStepRecord pendingRecord = new JobStepRecord(stepId, batchId, "doc-xyz", "PENDING", 1L, new UUID[0]);

        when(jobStepDao.findActiveBatchesWithPendingCount()).thenReturn(List.of(new JobStepDao.BatchPriorityPending(batchId, 5, 1)));
        when(jobStepDao.findPendingStepsForBatches(anyList(), anyInt())).thenReturn(List.of(pendingRecord));
        when(jobStepDao.markStepAssigned(eq(stepId), anyLong(), anyString())).thenAnswer(inv -> {
            stepStatusMap.put(stepId, "IN_PROGRESS");
            return true;
        });

        when(jobStepDao.updateStatusIfEpochAtMost(eq(stepId), anyLong(), eq("DONE"), anyString())).thenAnswer(inv -> {
            stepStatusMap.put(stepId, "DONE");
            return true;
        });

        when(jdbc.update(anyString(), eq(stepId), eq("STEP_DONE_NOTIFY"), anyString())).thenAnswer(inv -> {
            outboxList.add(Map.of("event_id", UUID.randomUUID(), "aggregate_id", stepId, "event_type", "STEP_DONE_NOTIFY", "payload", inv.getArgument(3), "published", false));
            return 1;
        });

        // 1. Dispatch
        StepDispatcher dispatcher = new StepDispatcher(jobStepDao, null, kafkaTemplate, objectMapper);
        dispatcher.dispatchPendingSteps();

        // Verify markStepAssigned called and message sent to Kafka topic
        assertEquals("IN_PROGRESS", stepStatusMap.get(stepId));
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("orchestrator-assign-step"), eq(stepId.toString()), messageCaptor.capture());

        String sentMsg = messageCaptor.getValue();
        AssignStepEvent assignEvent = objectMapper.readValue(sentMsg, AssignStepEvent.class);
        assertEquals(stepId.toString(), assignEvent.stepId());
        assertEquals("doc-xyz", assignEvent.documentId());

        // 2. Consume step result simulation
        StepResultEvent resultEvent = new StepResultEvent(
                assignEvent.traceId(),
                batchId.toString(),
                stepId.toString(),
                "{\"status\":\"SUCCESS\",\"data\":\"mock-output\"}",
                assignEvent.epoch()
        );
        String resultMsg = objectMapper.writeValueAsString(resultEvent);

        StepResultKafkaListener listener = new StepResultKafkaListener(stepService, objectMapper);
        listener.onStepResult(resultMsg);

        // Verify step moved to DONE
        assertEquals("DONE", stepStatusMap.get(stepId));
        assertEquals(1, outboxList.size());
        verify(jobStepDao).updateStatusIfEpochAtMost(eq(stepId), anyLong(), eq("DONE"), anyString());
    }
}
