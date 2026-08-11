package com.nganhcc.orchestration.worker.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State {
        CLOSED, OPEN, HALF_OPEN
    }

    public static class CircuitOpenException extends RuntimeException {
        public CircuitOpenException(String message) {
            super(message);
        }
    }

    private final int failureThreshold;
    private final long cooldownMs;
    private final int halfOpenMaxAttempts;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong openedAt = new AtomicLong(0);
    private final AtomicInteger halfOpenAttempts = new AtomicInteger(0);

    public CircuitBreaker(int failureThreshold, long cooldownMs, int halfOpenMaxAttempts) {
        this.failureThreshold = failureThreshold;
        this.cooldownMs = cooldownMs;
        this.halfOpenMaxAttempts = halfOpenMaxAttempts;
    }

    public CircuitBreaker() {
        this(5, 10000L, 1);
    }

    public State getState() {
        // Check transition from OPEN to HALF_OPEN based on cooldown expiration
        if (state.get() == State.OPEN) {
            long openTime = openedAt.get();
            if (System.currentTimeMillis() - openTime >= cooldownMs) {
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    log.info("CircuitBreaker state transitioned from OPEN to HALF_OPEN due to cooldown expiry");
                    halfOpenAttempts.set(0);
                }
            }
        }
        return state.get();
    }

    public int getFailureCount() {
        return consecutiveFailures.get();
    }

    public void reset() {
        state.set(State.CLOSED);
        consecutiveFailures.set(0);
        openedAt.set(0);
        halfOpenAttempts.set(0);
        log.info("CircuitBreaker manually reset to CLOSED");
    }

    public <T> T execute(Supplier<T> supplier) {
        State currentState = getState();

        if (currentState == State.OPEN) {
            throw new CircuitOpenException("Circuit is OPEN. LLM call rejected.");
        }

        if (currentState == State.HALF_OPEN) {
            if (halfOpenAttempts.incrementAndGet() > halfOpenMaxAttempts) {
                throw new CircuitOpenException("Circuit is HALF_OPEN. Probe rate limit exceeded.");
            }
            log.info("CircuitBreaker in HALF_OPEN. Attempting probe LLM call.");
        }

        try {
            T result = supplier.get();
            onSuccess();
            return result;
        } catch (Exception e) {
            onFailure(e);
            throw e;
        }
    }

    private void onSuccess() {
        State oldState = state.getAndSet(State.CLOSED);
        consecutiveFailures.set(0);
        openedAt.set(0);
        halfOpenAttempts.set(0);
        if (oldState != State.CLOSED) {
            log.info("CircuitBreaker state transitioned to CLOSED after successful call");
        }
    }

    private void onFailure(Exception e) {
        int failures = consecutiveFailures.incrementAndGet();
        State currentState = state.get();
        if (currentState == State.CLOSED) {
            if (failures >= failureThreshold) {
                if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                    openedAt.set(System.currentTimeMillis());
                    log.error("CircuitBreaker state transitioned from CLOSED to OPEN after {} consecutive failures. Error: {}", failures, e.getMessage());
                }
            } else {
                log.warn("CircuitBreaker consecutive failures: {}/{} (state: CLOSED). Error: {}", failures, failureThreshold, e.getMessage());
            }
        } else if (currentState == State.HALF_OPEN) {
            if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                openedAt.set(System.currentTimeMillis());
                log.error("CircuitBreaker state transitioned from HALF_OPEN to OPEN after probe failure. Error: {}", e.getMessage());
            }
        }
    }
}
