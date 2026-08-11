package com.nganhcc.orchestration.worker.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MockLlmClient {

    private static final Logger log = LoggerFactory.getLogger(MockLlmClient.class);

    private volatile boolean failureMode = false;
    private volatile int failureDelayMs = 0;

    public void setFailureMode(boolean enabled, int delayMs) {
        this.failureMode = enabled;
        this.failureDelayMs = delayMs;
        log.info("MockLlmClient: Fault injection updated. failureMode={}, failureDelayMs={}", enabled, delayMs);
    }

    public boolean isFailureMode() {
        return failureMode;
    }

    public int getFailureDelayMs() {
        return failureDelayMs;
    }

    public String processDocument(String stepId, String documentId) {
        log.info("MockLlmClient: Calling mock LLM provider for stepId={} documentId={}", stepId, documentId);
        
        if (failureMode) {
            if (failureDelayMs > 0) {
                try {
                    Thread.sleep(failureDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            throw new RuntimeException("MockLlmClient simulated failure");
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return String.format("{\"status\":\"SUCCESS\",\"extractedData\":\"extracted-content-for-%s\",\"stepId\":\"%s\"}", documentId, stepId);
    }
}
