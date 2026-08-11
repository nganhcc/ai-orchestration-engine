package com.nganhcc.orchestration.orchestrator.service;

import com.nganhcc.orchestration.orchestrator.dao.JobStepDao;
import com.nganhcc.orchestration.orchestrator.metrics.StepMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

class StepReaperTest {

    private JobStepDao jobStepDao;
    private StepMetrics stepMetrics;
    private StepReaper stepReaper;

    @BeforeEach
    void setUp() {
        jobStepDao = Mockito.mock(JobStepDao.class);
        stepMetrics = Mockito.mock(StepMetrics.class);
        stepReaper = new StepReaper(jobStepDao, stepMetrics, null, 30L);
    }

    @Test
    void testReapStaleSteps_TriggersDaoAndMetrics() {
        when(jobStepDao.reapStaleSteps(30L)).thenReturn(3);

        stepReaper.reapStaleSteps();

        verify(jobStepDao).reapStaleSteps(30L);
        verify(stepMetrics).incrementStepsReassigned(3);
    }
}
