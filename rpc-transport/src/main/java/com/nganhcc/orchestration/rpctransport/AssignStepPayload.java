package com.nganhcc.orchestration.rpctransport;

import java.util.UUID;

public record AssignStepPayload(
        UUID traceId,
        String jobId,
        String stepId,
        byte[] payload   // nội dung nghiệp vụ, ví dụ JSON cho LLM
) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AssignStepPayload other)) return false;
        return traceId.equals(other.traceId)
                && jobId.equals(other.jobId)
                && stepId.equals(other.stepId)
                && java.util.Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(traceId, jobId, stepId);
        result = 31 * result + java.util.Arrays.hashCode(payload);
        return result;
    }
}