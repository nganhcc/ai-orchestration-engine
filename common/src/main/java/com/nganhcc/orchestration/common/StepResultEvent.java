package com.nganhcc.orchestration.common;

public record StepResultEvent(
    String traceId,
    String batchId,
    String stepId,
    String resultJson,
    long epoch
) {}
