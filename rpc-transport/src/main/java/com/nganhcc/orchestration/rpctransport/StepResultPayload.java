package com.nganhcc.orchestration.rpctransport;

import java.util.UUID;

public record StepResultPayload(UUID traceId, String jobId, String stepId, byte[] result) {
}
