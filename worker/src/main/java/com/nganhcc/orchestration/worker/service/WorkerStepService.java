package com.nganhcc.orchestration.worker.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class WorkerStepService {

    private static final Logger log = LoggerFactory.getLogger(WorkerStepService.class);

    private final MockLlmClient mockLlmClient;
    private final JdbcOperations jdbc;
    private final Map<UUID, String> inMemoryDedup = new ConcurrentHashMap<>();
    private final CircuitBreaker circuitBreaker = new CircuitBreaker(5, 10000L, 1);

    @Autowired
    public WorkerStepService(MockLlmClient mockLlmClient, @Autowired(required = false) JdbcOperations jdbc) {
        this.mockLlmClient = mockLlmClient;
        this.jdbc = jdbc;
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    public String processStep(UUID stepId, String jobId, String documentId) {
        // Check in-memory dedup first
        if (inMemoryDedup.containsKey(stepId)) {
            log.info("WorkerStepService: Dedup HIT (in-memory) for stepId={}", stepId);
            return inMemoryDedup.get(stepId);
        }

        // Check DB dedup if JDBC available
        if (jdbc != null) {
            try {
                List<Map<String, Object>> rows = jdbc.queryForList(
                        "SELECT status, result FROM worker_step_status WHERE step_id = ?", stepId);
                if (!rows.isEmpty()) {
                    Map<String, Object> row = rows.get(0);
                    if ("DONE".equals(row.get("status"))) {
                        Object resObj = row.get("result");
                        String cachedResult = resObj != null ? resObj.toString() : "{}";
                        log.info("WorkerStepService: Dedup HIT (DB) for stepId={}", stepId);
                        inMemoryDedup.put(stepId, cachedResult);
                        return cachedResult;
                    }
                }
            } catch (Exception e) {
                log.warn("WorkerStepService DB check failed, falling back to execution: {}", e.getMessage());
            }
        }

        // Execute Mock LLM call wrapped in Circuit Breaker
        String resultJson = circuitBreaker.execute(() -> mockLlmClient.processDocument(stepId.toString(), documentId));

        // Store in DB & in-memory
        inMemoryDedup.put(stepId, resultJson);

        if (jdbc != null) {
            try {
                jdbc.update(
                        "INSERT INTO worker_step_status(step_id, status, result, updated_at) VALUES(?, 'DONE', CAST(? AS jsonb), now()) "
                                + "ON CONFLICT (step_id) DO UPDATE SET status = 'DONE', result = EXCLUDED.result, updated_at = now()",
                        stepId, resultJson
                );
            } catch (Exception e) {
                log.warn("WorkerStepService DB save failed for stepId={}: {}", stepId, e.getMessage());
            }
        }

        return resultJson;
    }
}
