package com.nganhcc.orchestration.orchestrator.service;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class DagSchedulerTest {

    @Test
    public void testUnblockDependentsLinear() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        UUID c = UUID.randomUUID();

        Map<UUID, DagScheduler.DagStep> steps = new HashMap<>();
        steps.put(a, new DagScheduler.DagStep(a, List.of(), "DONE"));
        steps.put(b, new DagScheduler.DagStep(b, List.of(a), "BLOCKED"));
        steps.put(c, new DagScheduler.DagStep(c, List.of(b), "BLOCKED"));

        List<UUID> unblocked = DagScheduler.unblockDependents(steps, a);
        assertEquals(List.of(b), unblocked);
    }

    @Test
    public void testUnblockDependentsDiamond() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        UUID c = UUID.randomUUID();
        UUID d = UUID.randomUUID();

        Map<UUID, DagScheduler.DagStep> steps = new HashMap<>();
        steps.put(a, new DagScheduler.DagStep(a, List.of(), "DONE"));
        steps.put(b, new DagScheduler.DagStep(b, List.of(a), "DONE"));
        steps.put(c, new DagScheduler.DagStep(c, List.of(a), "BLOCKED"));
        steps.put(d, new DagScheduler.DagStep(d, List.of(b, c), "BLOCKED"));

        // When b is DONE, but c is still BLOCKED, d should not be unblocked
        List<UUID> unblocked = DagScheduler.unblockDependents(steps, b);
        assertTrue(unblocked.isEmpty());

        // Now change c to DONE
        steps.put(c, new DagScheduler.DagStep(c, List.of(a), "DONE"));
        unblocked = DagScheduler.unblockDependents(steps, c);
        assertEquals(List.of(d), unblocked);
    }

    @Test
    public void testCycleDetection() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        UUID c = UUID.randomUUID();

        // Cyclic: A -> B -> C -> A
        Map<UUID, DagScheduler.DagStep> steps = new HashMap<>();
        steps.put(a, new DagScheduler.DagStep(a, List.of(c), "BLOCKED"));
        steps.put(b, new DagScheduler.DagStep(b, List.of(a), "BLOCKED"));
        steps.put(c, new DagScheduler.DagStep(c, List.of(b), "BLOCKED"));

        assertTrue(DagScheduler.hasCycle(steps));

        // Acyclic: A -> B -> C
        Map<UUID, DagScheduler.DagStep> steps2 = new HashMap<>();
        steps2.put(a, new DagScheduler.DagStep(a, List.of(), "BLOCKED"));
        steps2.put(b, new DagScheduler.DagStep(b, List.of(a), "BLOCKED"));
        steps2.put(c, new DagScheduler.DagStep(c, List.of(b), "BLOCKED"));

        assertFalse(DagScheduler.hasCycle(steps2));
    }
}
