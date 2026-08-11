package com.nganhcc.orchestration.worker.web;

import com.nganhcc.orchestration.worker.service.CircuitBreaker;
import com.nganhcc.orchestration.worker.service.MockLlmClient;
import com.nganhcc.orchestration.worker.service.WorkerStepService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/worker/status")
public class WorkerStatusController {

    private final WorkerStepService workerStepService;
    private final MockLlmClient mockLlmClient;

    @Autowired
    public WorkerStatusController(WorkerStepService workerStepService, MockLlmClient mockLlmClient) {
        this.workerStepService = workerStepService;
        this.mockLlmClient = mockLlmClient;
    }

    @GetMapping("/circuit-breaker")
    public ResponseEntity<?> getCircuitBreakerStatus() {
        CircuitBreaker cb = workerStepService.getCircuitBreaker();
        return ResponseEntity.ok(Map.of(
                "state", cb.getState().name(),
                "consecutiveFailures", cb.getFailureCount()
        ));
    }

    @PostMapping("/fault-injection")
    public ResponseEntity<?> configureFaultInjection(@RequestBody FaultInjectionRequest req) {
        mockLlmClient.setFailureMode(req.enabled, req.delayMs);
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "failureMode", mockLlmClient.isFailureMode(),
                "failureDelayMs", mockLlmClient.getFailureDelayMs()
        ));
    }

    public static class FaultInjectionRequest {
        public boolean enabled;
        public int delayMs;
    }
}
