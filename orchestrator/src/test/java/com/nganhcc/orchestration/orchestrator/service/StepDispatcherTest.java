package com.nganhcc.orchestration.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nganhcc.orchestration.orchestrator.dao.JobStepDao;
import com.nganhcc.orchestration.orchestrator.dao.JobStepDao.JobStepRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class StepDispatcherTest {

    private JobStepDao jobStepDao;
    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper objectMapper;
    private StepDispatcher stepDispatcher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        jobStepDao = Mockito.mock(JobStepDao.class);
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        objectMapper = new ObjectMapper();
        stepDispatcher = new StepDispatcher(jobStepDao, null, kafkaTemplate, objectMapper);
    }

    @Test
    void testDispatchPendingSteps_NoActiveBatches_DoesNothing() {
        when(jobStepDao.findActiveBatchesWithPendingCount()).thenReturn(Collections.emptyList());
        stepDispatcher.dispatchPendingSteps();
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void testDispatchPendingSteps_DispatchesToKafka() {
        UUID batchId = UUID.randomUUID();
        UUID stepId = UUID.randomUUID();
        JobStepRecord record = new JobStepRecord(stepId, batchId, "doc-1", "PENDING", 1L, new UUID[0]);

        when(jobStepDao.findActiveBatchesWithPendingCount()).thenReturn(List.of(new JobStepDao.BatchPriorityPending(batchId, 5, 1)));
        when(jobStepDao.findPendingStepsForBatches(anyList(), anyInt())).thenReturn(List.of(record));
        when(jobStepDao.markStepAssigned(eq(stepId), anyLong(), anyString())).thenReturn(true);

        stepDispatcher.dispatchPendingSteps();

        verify(jobStepDao).markStepAssigned(eq(stepId), anyLong(), anyString());
        verify(kafkaTemplate).send(eq("orchestrator-assign-step"), eq(stepId.toString()), anyString());
    }
}
