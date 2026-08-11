package com.nganhcc.orchestration.common;

public record AssignStepEvent(
    String traceId,
    String batchId,
    String stepId,
    String documentId,
    long epoch
) {}
