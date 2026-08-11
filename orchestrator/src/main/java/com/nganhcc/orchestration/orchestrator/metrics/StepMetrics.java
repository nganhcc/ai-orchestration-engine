package com.nganhcc.orchestration.orchestrator.metrics;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class StepMetrics {

    private final AtomicLong stepsRerunCounter = new AtomicLong(0);
    private final AtomicLong stepsReassignedCounter = new AtomicLong(0);
    private final AtomicLong stepsCompletedCounter = new AtomicLong(0);
    private final AtomicLong batchesCreatedCounter = new AtomicLong(0);

    public void incrementStepsRerun() {
        stepsRerunCounter.incrementAndGet();
    }

    public void incrementStepsReassigned(double amount) {
        stepsReassignedCounter.addAndGet((long) amount);
    }

    public void incrementStepsCompleted() {
        stepsCompletedCounter.incrementAndGet();
    }

    public void incrementBatchesCreated() {
        batchesCreatedCounter.incrementAndGet();
    }

    public long getStepsRerun() {
        return stepsRerunCounter.get();
    }

    public long getStepsReassigned() {
        return stepsReassignedCounter.get();
    }

    public long getStepsCompleted() {
        return stepsCompletedCounter.get();
    }

    public long getBatchesCreated() {
        return batchesCreatedCounter.get();
    }
}
