package com.nganhcc.orchestration.orchestrator.service;

import java.util.*;

public class PriorityScheduler {

    public record BatchInfo(int priority, int pendingCount) {}

    /**
     * DRR implementation.
     * Input: map batchId -> BatchInfo(priority, pendingCount), currentDeficits (modified in-place)
     * Output: ordered list batchId specifying scheduling sequence for pending steps.
     */
    public static List<UUID> nextDispatchOrder(Map<UUID, BatchInfo> activeBatches, Map<UUID, Long> deficits, int limit) {
        List<UUID> result = new ArrayList<>();
        if (activeBatches.isEmpty()) {
            return result;
        }

        // We clean up deficits for batches that are no longer active
        deficits.keySet().retainAll(activeBatches.keySet());

        List<UUID> batchIds = new ArrayList<>(activeBatches.keySet());
        // Sort by ID to ensure deterministic processing order
        batchIds.sort(Comparator.comparing(UUID::toString));

        // Copy pending counts to track dispatch availability within this call
        Map<UUID, Integer> remainingPending = new HashMap<>();
        for (Map.Entry<UUID, BatchInfo> entry : activeBatches.entrySet()) {
            remainingPending.put(entry.getKey(), entry.getValue().pendingCount());
        }

        boolean workDone = true;
        while (result.size() < limit && workDone) {
            workDone = false;
            for (UUID batchId : batchIds) {
                if (result.size() >= limit) {
                    break;
                }

                int pending = remainingPending.getOrDefault(batchId, 0);
                if (pending <= 0) {
                    continue;
                }

                BatchInfo info = activeBatches.get(batchId);
                // Quantum = (11 - priority). High priority (1) gets 10, Low priority (10) gets 1.
                long quantum = 11L - info.priority();
                long currentDeficit = deficits.getOrDefault(batchId, 0L) + quantum;

                // We can dispatch up to currentDeficit steps
                int dispatchedFromThisBatch = 0;
                while (currentDeficit >= 1 && pending > 0 && result.size() < limit) {
                    result.add(batchId);
                    pending--;
                    currentDeficit--;
                    dispatchedFromThisBatch++;
                    workDone = true;
                }

                remainingPending.put(batchId, pending);
                deficits.put(batchId, currentDeficit);
            }
        }

        return result;
    }
}
