package com.nganhcc.orchestration.worker.service;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CircuitBreakerTest {

    @Test
    void testClosedStateInitially() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000L, 1);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }

    @Test
    void testTransitionsToOpenAfterThresholdFailures() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000L, 1);

        // Fail 1
        assertThrows(RuntimeException.class, () -> cb.execute(() -> {
            throw new RuntimeException("fail");
        }));
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertEquals(1, cb.getFailureCount());

        // Fail 2
        assertThrows(RuntimeException.class, () -> cb.execute(() -> {
            throw new RuntimeException("fail");
        }));
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertEquals(2, cb.getFailureCount());

        // Fail 3 -> should transition to OPEN
        assertThrows(RuntimeException.class, () -> cb.execute(() -> {
            throw new RuntimeException("fail");
        }));
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        // Further executions should throw CircuitOpenException immediately without calling supplier
        AtomicInteger callCount = new AtomicInteger(0);
        assertThrows(CircuitBreaker.CircuitOpenException.class, () -> cb.execute(() -> {
            callCount.incrementAndGet();
            return "ok";
        }));
        assertEquals(0, callCount.get());
    }

    @Test
    void testTransitionsToHalfOpenAfterCooldown() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(2, 200L, 1);

        // Fail 1
        assertThrows(RuntimeException.class, () -> cb.execute(() -> {
            throw new RuntimeException("fail");
        }));
        // Fail 2 -> OPEN
        assertThrows(RuntimeException.class, () -> cb.execute(() -> {
            throw new RuntimeException("fail");
        }));
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        // Wait for cooldown
        Thread.sleep(250);

        // State check should transition to HALF_OPEN
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
    }

    @Test
    void testHalfOpenProbeSuccessResetsToClosed() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(2, 100L, 1);

        assertThrows(RuntimeException.class, () -> cb.execute(() -> { throw new RuntimeException("fail"); }));
        assertThrows(RuntimeException.class, () -> cb.execute(() -> { throw new RuntimeException("fail"); }));
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        Thread.sleep(150);

        // Execute successful call in HALF_OPEN
        String res = cb.execute(() -> "success");
        assertEquals("success", res);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertEquals(0, cb.getFailureCount());
    }

    @Test
    void testHalfOpenProbeFailureTransitsToOpen() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker(2, 100L, 1);

        assertThrows(RuntimeException.class, () -> cb.execute(() -> { throw new RuntimeException("fail"); }));
        assertThrows(RuntimeException.class, () -> cb.execute(() -> { throw new RuntimeException("fail"); }));
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        Thread.sleep(150);

        // Probe fails -> transits back to OPEN
        assertThrows(RuntimeException.class, () -> cb.execute(() -> {
            throw new RuntimeException("probe fail");
        }));
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
    }

    @Test
    void testResetCounterOnSuccess() {
        CircuitBreaker cb = new CircuitBreaker(3, 1000L, 1);

        assertThrows(RuntimeException.class, () -> cb.execute(() -> { throw new RuntimeException("fail"); }));
        assertThrows(RuntimeException.class, () -> cb.execute(() -> { throw new RuntimeException("fail"); }));
        assertEquals(2, cb.getFailureCount());

        // Successful call should reset count to 0
        String res = cb.execute(() -> "ok");
        assertEquals("ok", res);
        assertEquals(0, cb.getFailureCount());
    }

    @Test
    void testConcurrencyAndThreadSafety() throws InterruptedException {
        int threadCount = 20;
        CircuitBreaker cb = new CircuitBreaker(5, 10000L, 1);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger successfulCalls = new AtomicInteger(0);
        AtomicInteger circuitOpenExceptions = new AtomicInteger(0);
        AtomicInteger runtimeExceptions = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    latch.await();
                    cb.execute(() -> {
                        successfulCalls.incrementAndGet();
                        throw new RuntimeException("concurrent failure");
                    });
                } catch (CircuitBreaker.CircuitOpenException e) {
                    circuitOpenExceptions.incrementAndGet();
                } catch (RuntimeException e) {
                    runtimeExceptions.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        latch.countDown();
        doneLatch.await();
        executor.shutdown();

        // Exactly 5 calls should succeed in running supplier (and throwing RuntimeException)
        // before transitioning to OPEN.
        // The rest should throw CircuitOpenException.
        assertEquals(5, runtimeExceptions.get());
        assertEquals(5, successfulCalls.get());
        assertEquals(15, circuitOpenExceptions.get());
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
    }
}
