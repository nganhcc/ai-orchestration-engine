package com.nganhcc.orchestration.orchestrator.service;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class PrioritySchedulerTest {

    @Test
    public void testDRRBasic() {
        UUID batchA = UUID.randomUUID(); // priority 1 -> quantum 10
        UUID batchB = UUID.randomUUID(); // priority 5 -> quantum 6

        Map<UUID, PriorityScheduler.BatchInfo> active = new HashMap<>();
        active.put(batchA, new PriorityScheduler.BatchInfo(1, 20));
        active.put(batchB, new PriorityScheduler.BatchInfo(5, 20));

        Map<UUID, Long> deficits = new HashMap<>();

        // limit 10
        List<UUID> order = PriorityScheduler.nextDispatchOrder(active, deficits, 10);

        // batchA gets 10 steps because quantum is 10.
        // But let's check order. batchA and batchB are sorted alphabetically.
        // Let's print or assert that batchA got 10 and batchB got 0 because batchA's quantum was satisfied first if it was evaluated first.
        // Actually: batchIds are processed in order.
        // Let's say batchA has UUID that is smaller than batchB:
        UUID first = batchA.toString().compareTo(batchB.toString()) < 0 ? batchA : batchB;
        UUID second = first == batchA ? batchB : batchA;

        // Reset deficits and active to make it deterministic
        active.clear();
        active.put(first, new PriorityScheduler.BatchInfo(1, 20)); // quantum 10
        active.put(second, new PriorityScheduler.BatchInfo(5, 20)); // quantum 6
        deficits.clear();

        List<UUID> orderDeterministic = PriorityScheduler.nextDispatchOrder(active, deficits, 12);
        // first should dispatch 10, then second should dispatch 2 (to satisfy limit 12)
        int firstCount = 0;
        int secondCount = 0;
        for (UUID id : orderDeterministic) {
            if (id.equals(first)) firstCount++;
            else if (id.equals(second)) secondCount++;
        }
        assertEquals(10, firstCount);
        assertEquals(2, secondCount);
    }
}
