package com.nganhcc.orchestration.orchestrator.web.dto;

import java.util.List;

public class BatchRequestDto {
    private String idempotencyKey;
    private List<String> documents;
    private Integer priority;

    public BatchRequestDto() {
    }

    public BatchRequestDto(String idempotencyKey, List<String> documents, Integer priority) {
        this.idempotencyKey = idempotencyKey;
        this.documents = documents;
        this.priority = priority;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public List<String> getDocuments() {
        return documents;
    }

    public void setDocuments(List<String> documents) {
        this.documents = documents;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }
}
