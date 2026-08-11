package com.nganhcc.orchestration.worker.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WorkerStepServiceTest {

    private MockLlmClient mockLlmClient;
    private WorkerStepService workerStepService;

    @BeforeEach
    void setUp() {
        mockLlmClient = new MockLlmClient();
        workerStepService = new WorkerStepService(mockLlmClient, null);
    }

    @Test
    void testProcessStep_FirstCallExecutesAndReturnsResult() {
        UUID stepId = UUID.randomUUID();
        String result = workerStepService.processStep(stepId, "job-1", "doc-100");

        assertNotNull(result);
        assertTrue(result.contains("extracted-content-for-doc-100"));
        assertTrue(result.contains(stepId.toString()));
    }

    @Test
    void testProcessStep_SecondCallUsesInMemoryDedup() {
        UUID stepId = UUID.randomUUID();
        String res1 = workerStepService.processStep(stepId, "job-1", "doc-100");
        String res2 = workerStepService.processStep(stepId, "job-1", "doc-100");

        assertEquals(res1, res2);
    }
}
