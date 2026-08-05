package com.nganhcc.orchestration.orchestrator.web;

import com.nganhcc.orchestration.orchestrator.service.StepService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/internal/steps")
public class StepController {

    private final StepService stepService;

    public StepController(StepService stepService) {
        this.stepService = stepService;
    }

    @PostMapping("/{stepId}/done")
    public ResponseEntity<?> markDone(@PathVariable("stepId") UUID stepId, @RequestBody MarkDoneRequest req) {
        boolean ok = stepService.markStepDone(stepId, req.epoch, req.resultJson);
        if (ok) return ResponseEntity.ok().build();
        return ResponseEntity.status(409).body("stale-epoch");
    }

    public static class MarkDoneRequest {
        public long epoch;
        public String resultJson;
    }
}
