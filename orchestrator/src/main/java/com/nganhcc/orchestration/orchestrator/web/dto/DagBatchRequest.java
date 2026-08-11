package com.nganhcc.orchestration.orchestrator.web.dto;

import java.util.List;

public class DagBatchRequest {
    private String idempotencyKey;
    private Integer priority;
    private List<StepDto> steps;

    public DagBatchRequest() {
    }

    public DagBatchRequest(String idempotencyKey, Integer priority, List<StepDto> steps) {
        this.idempotencyKey = idempotencyKey;
        this.priority = priority;
        this.steps = steps;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public List<StepDto> getSteps() {
        return steps;
    }

    public void setSteps(List<StepDto> steps) {
        this.steps = steps;
    }

    public static class StepDto {
        private String name;
        private String documentId;
        private List<String> dependsOn;

        public StepDto() {
        }

        public StepDto(String name, String documentId, List<String> dependsOn) {
            this.name = name;
            this.documentId = documentId;
            this.dependsOn = dependsOn;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDocumentId() {
            return documentId;
        }

        public void setDocumentId(String documentId) {
            this.documentId = documentId;
        }

        public List<String> getDependsOn() {
            return dependsOn;
        }

        public void setDependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn;
        }
    }
}
