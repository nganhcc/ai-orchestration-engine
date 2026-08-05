package com.nganhcc.orchestration.orchestrator.service;

import com.nganhcc.orchestration.orchestrator.dao.JobStepDao;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class StepServiceTest {

    @Test
    void markStepDone_insertsOutbox_whenDaoSucceeds() {
        JobStepDao dao = mock(JobStepDao.class);
        JdbcOperations jdbc = mock(JdbcOperations.class);
        StepService svc = new StepService(dao, jdbc);

        UUID stepId = UUID.randomUUID();
        when(dao.updateStatusIfEpochAtMost(stepId, 5L, "DONE", "{}")).thenReturn(true);

        boolean ok = svc.markStepDone(stepId, 5L, "{}");
        assertTrue(ok);
        verify(dao).updateStatusIfEpochAtMost(stepId, 5L, "DONE", "{}");
        verify(jdbc).update(anyString(), eq(stepId.toString()), eq("STEP_DONE_NOTIFY"), eq("{}"));
    }

    @Test
    void markStepDone_returnsFalse_whenDaoRejects() {
        JobStepDao dao = mock(JobStepDao.class);
        JdbcOperations jdbc = mock(JdbcOperations.class);
        StepService svc = new StepService(dao, jdbc);

        UUID stepId = UUID.randomUUID();
        when(dao.updateStatusIfEpochAtMost(stepId, 1L, "DONE", "{}")).thenReturn(false);

        boolean ok = svc.markStepDone(stepId, 1L, "{}");
        assertFalse(ok);
        verify(dao).updateStatusIfEpochAtMost(stepId, 1L, "DONE", "{}");
        verifyNoInteractions(jdbc);
    }
}
