package com.nganhcc.orchestration.orchestrator.web;

import com.nganhcc.orchestration.orchestrator.metrics.StepMetrics;
import com.nganhcc.orchestration.orchestrator.raft.RaftClusterBootstrap;
import com.nganhcc.orchestration.orchestrator.web.dto.BatchRequestDto;
import com.nganhcc.orchestration.raftcore.RaftNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;


import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class OrchestratorController {

    private final org.springframework.jdbc.core.JdbcTemplate jdbc;
    private final StepMetrics stepMetrics;
    private final RaftClusterBootstrap raftClusterBootstrap;

    @Autowired
    public OrchestratorController(JdbcTemplate jdbc,
                                  @Autowired(required = false) StepMetrics stepMetrics,
                                  @Autowired(required = false) RaftClusterBootstrap raftClusterBootstrap) {
        this.jdbc = jdbc;
        this.stepMetrics = stepMetrics;
        this.raftClusterBootstrap = raftClusterBootstrap;
    }

    @PostMapping("/batches")
    public ResponseEntity<?> createBatch(
            @RequestHeader(value = "Idempotency-Key", required = false) String headerIdempotencyKey,
            @RequestBody(required = false) BatchRequestDto req) {

        RaftNode raftNode = raftClusterBootstrap != null ? raftClusterBootstrap.raftNode() : null;
        if (raftNode != null && !raftNode.isLeader()) {
            return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                    .header("Location", "/api/batches")
                    .body(Map.of("error", "Not Raft leader", "currentTerm", raftNode.state().currentTerm));
        }

        long epoch = (raftNode != null && raftNode.isLeader()) ? raftNode.epoch() : 1L;

        String idempotencyKey = headerIdempotencyKey != null ? headerIdempotencyKey
                : (req != null ? req.getIdempotencyKey() : null);

        // Idempotency check
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            List<Map<String, Object>> existing = jdbc.queryForList(
                    "SELECT batch_id, status FROM batch_job WHERE idempotency_key = ?", idempotencyKey);
            if (!existing.isEmpty()) {
                Map<String, Object> row = existing.get(0);
                return ResponseEntity.ok(Map.of(
                        "batchId", row.get("batch_id").toString(),
                        "status", row.get("status"),
                        "duplicate", true
                ));
            }
        }

        List<String> docs = (req != null && req.getDocuments() != null && !req.getDocuments().isEmpty())
                ? req.getDocuments()
                : List.of("doc-1");
        int priority = (req != null && req.getPriority() != null) ? req.getPriority() : 5;

        UUID batchId = UUID.randomUUID();

        // Insert batch_job
        jdbc.update(
                "INSERT INTO batch_job(batch_id, idempotency_key, status, total_documents, priority) VALUES(?, ?, 'RUNNING', ?, ?)",
                batchId, idempotencyKey, docs.size(), priority
        );

        // Insert job_step for each document
        for (String doc : docs) {
            UUID stepId = UUID.randomUUID();
            jdbc.update(
                    "INSERT INTO job_step(step_id, batch_id, document_id, status, leader_epoch) VALUES(?, ?, ?, 'PENDING', ?)",
                    stepId, batchId, doc, epoch
            );
        }

        if (stepMetrics != null) {
            stepMetrics.incrementBatchesCreated();
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "batchId", batchId.toString(),
                "totalDocuments", docs.size(),
                "status", "RUNNING"
        ));
    }

    @PostMapping("/batches/dag")
    public ResponseEntity<?> createDagBatch(
            @RequestHeader(value = "Idempotency-Key", required = false) String headerIdempotencyKey,
            @RequestBody com.nganhcc.orchestration.orchestrator.web.dto.DagBatchRequest req) {

        RaftNode raftNode = raftClusterBootstrap != null ? raftClusterBootstrap.raftNode() : null;
        if (raftNode != null && !raftNode.isLeader()) {
            return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                    .header("Location", "/api/batches/dag")
                    .body(Map.of("error", "Not Raft leader", "currentTerm", raftNode.state().currentTerm));
        }

        long epoch = (raftNode != null && raftNode.isLeader()) ? raftNode.epoch() : 1L;

        String idempotencyKey = headerIdempotencyKey != null ? headerIdempotencyKey
                : (req != null ? req.getIdempotencyKey() : null);

        // Idempotency check
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            List<Map<String, Object>> existing = jdbc.queryForList(
                    "SELECT batch_id, status FROM batch_job WHERE idempotency_key = ?", idempotencyKey);
            if (!existing.isEmpty()) {
                Map<String, Object> row = existing.get(0);
                return ResponseEntity.ok(Map.of(
                        "batchId", row.get("batch_id").toString(),
                        "status", row.get("status"),
                        "duplicate", true
                ));
            }
        }

        if (req == null || req.getSteps() == null || req.getSteps().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Steps must not be empty"));
        }

        int priority = req.getPriority() != null ? req.getPriority() : 5;
        UUID batchId = UUID.randomUUID();

        // 1. Assign UUID to each step name
        Map<String, UUID> nameToId = new java.util.HashMap<>();
        for (com.nganhcc.orchestration.orchestrator.web.dto.DagBatchRequest.StepDto step : req.getSteps()) {
            if (step.getName() == null || step.getName().isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Step name must not be empty"));
            }
            nameToId.put(step.getName(), UUID.randomUUID());
        }

        // 2. Build DAG validation map
        Map<UUID, com.nganhcc.orchestration.orchestrator.service.DagScheduler.DagStep> validationMap = new java.util.HashMap<>();
        for (com.nganhcc.orchestration.orchestrator.web.dto.DagBatchRequest.StepDto step : req.getSteps()) {
            UUID id = nameToId.get(step.getName());
            List<UUID> deps = new java.util.ArrayList<>();
            if (step.getDependsOn() != null) {
                for (String depName : step.getDependsOn()) {
                    UUID depId = nameToId.get(depName);
                    if (depId == null) {
                        return ResponseEntity.badRequest().body(Map.of("error", "Dependency step '" + depName + "' not found in batch"));
                    }
                    deps.add(depId);
                }
            }
            validationMap.put(id, new com.nganhcc.orchestration.orchestrator.service.DagScheduler.DagStep(id, deps, "BLOCKED"));
        }

        // 3. Cycle Detection
        if (com.nganhcc.orchestration.orchestrator.service.DagScheduler.hasCycle(validationMap)) {
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(Map.of("error", "DAG has a cycle"));
        }

        // 4. Save batch_job
        jdbc.update(
                "INSERT INTO batch_job(batch_id, idempotency_key, status, total_documents, priority) VALUES(?, ?, 'RUNNING', ?, ?)",
                batchId, idempotencyKey, req.getSteps().size(), priority
        );

        // 5. Save job_steps
        for (com.nganhcc.orchestration.orchestrator.web.dto.DagBatchRequest.StepDto step : req.getSteps()) {
            UUID stepId = nameToId.get(step.getName());
            List<UUID> deps = new java.util.ArrayList<>();
            if (step.getDependsOn() != null) {
                for (String depName : step.getDependsOn()) {
                    deps.add(nameToId.get(depName));
                }
            }

            // Convert to SQL Array
            java.sql.Array sqlDepsArray = null;
            try {
                java.sql.Connection conn = java.util.Objects.requireNonNull(jdbc.getDataSource()).getConnection();
                sqlDepsArray = conn.createArrayOf("uuid", deps.toArray());
                conn.close();
            } catch (Exception e) {
                // Ignore, sqlDepsArray is null
            }

            // Roots are PENDING, dependents are BLOCKED
            String status = deps.isEmpty() ? "PENDING" : "BLOCKED";

            if (sqlDepsArray != null) {
                jdbc.update(
                        "INSERT INTO job_step(step_id, batch_id, document_id, depends_on, status, leader_epoch) VALUES(?, ?, ?, ?, ?, ?)",
                        stepId, batchId, step.getDocumentId(), sqlDepsArray, status, epoch
                );
            } else {
                // Fallback for H2 or if connection array creation failed
                // String concat array style
                StringBuilder sb = new StringBuilder("ARRAY[");
                for (int i = 0; i < deps.size(); i++) {
                    sb.append("'").append(deps.get(i).toString()).append("'::uuid");
                    if (i < deps.size() - 1) sb.append(",");
                }
                sb.append("]");
                String arrayLiteral = deps.isEmpty() ? "NULL" : sb.toString();

                jdbc.update(
                        "INSERT INTO job_step(step_id, batch_id, document_id, depends_on, status, leader_epoch) VALUES(?, ?, ?, " + (deps.isEmpty() ? "NULL" : "CAST(" + arrayLiteral + " AS uuid[])") + ", ?, ?)",
                        stepId, batchId, step.getDocumentId(), status, epoch
                );
            }
        }

        if (stepMetrics != null) {
            stepMetrics.incrementBatchesCreated();
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "batchId", batchId.toString(),
                "totalDocuments", req.getSteps().size(),
                "status", "RUNNING"
        ));
    }

    @GetMapping("/cluster/status")
    public ResponseEntity<?> getClusterStatus() {
        RaftNode raftNode = raftClusterBootstrap != null ? raftClusterBootstrap.raftNode() : null;
        if (raftNode == null) {
            return ResponseEntity.ok(Map.of("enabled", false));
        }
        return ResponseEntity.ok(Map.of(
                "enabled", true,
                "nodeId", raftNode.state().selfId,
                "role", raftNode.state().nodeState.name(),
                "term", raftNode.state().currentTerm,
                "epoch", raftNode.epoch(),
                "isLeader", raftNode.isLeader()
        ));
    }

    @GetMapping("/batches/{batchId}/steps")
    public ResponseEntity<?> getBatchSteps(@PathVariable("batchId") UUID batchId) {
        List<Map<String, Object>> steps = jdbc.queryForList(
                "SELECT step_id, document_id, status, retry_count, assigned_worker_id, leader_epoch, updated_at " +
                "FROM job_step WHERE batch_id = ? ORDER BY updated_at ASC", batchId);
        return ResponseEntity.ok(steps);
    }

    @GetMapping("/metrics")
    public ResponseEntity<?> getMetrics() {
        if (stepMetrics == null) {
            return ResponseEntity.ok(Map.of("error", "Metrics not available"));
        }
        return ResponseEntity.ok(Map.of(
                "batchesCreated", stepMetrics.getBatchesCreated(),
                "stepsCompleted", stepMetrics.getStepsCompleted(),
                "stepsReassigned", stepMetrics.getStepsReassigned(),
                "stepsRerun", stepMetrics.getStepsRerun()
        ));
    }

    @GetMapping("/batches")
    public ResponseEntity<?> getBatches() {
        List<Map<String, Object>> batches = jdbc.queryForList(
                "SELECT batch_id, idempotency_key, status, total_documents, priority, created_at " +
                "FROM batch_job ORDER BY created_at DESC");
        return ResponseEntity.ok(batches);
    }
}
