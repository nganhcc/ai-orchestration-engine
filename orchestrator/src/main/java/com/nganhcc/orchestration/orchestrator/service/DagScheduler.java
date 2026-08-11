package com.nganhcc.orchestration.orchestrator.service;

import java.util.*;

public class DagScheduler {

    public record DagStep(UUID stepId, List<UUID> dependsOn, String status) {}

    /**
     * Input: tập hợp step trong 1 batch, stepId vừa mark DONE
     * Output: list stepId nào chuyển từ BLOCKED → PENDING
     */
    public static List<UUID> unblockDependents(Map<UUID, DagStep> allSteps, UUID justCompletedStepId) {
        List<UUID> unblocked = new ArrayList<>();
        for (DagStep step : allSteps.values()) {
            if ("BLOCKED".equals(step.status()) && step.dependsOn() != null && step.dependsOn().contains(justCompletedStepId)) {
                // Check if all dependencies are DONE
                boolean allDone = true;
                for (UUID depId : step.dependsOn()) {
                    DagStep dep = allSteps.get(depId);
                    // Just completed step counts as DONE, or it's already marked DONE in allSteps
                    if (depId.equals(justCompletedStepId)) {
                        continue;
                    }
                    if (dep == null || !"DONE".equals(dep.status())) {
                        allDone = false;
                        break;
                    }
                }
                if (allDone) {
                    unblocked.add(step.stepId());
                }
            }
        }
        return unblocked;
    }

    /**
     * Input: danh sách DagStep cần validate
     * Output: true nếu acyclic, false nếu có cycle
     */
    public static boolean hasCycle(Map<UUID, DagStep> steps) {
        // DFS cycle detection using 3 states: 0 = unvisited, 1 = visiting, 2 = visited
        Map<UUID, Integer> visited = new HashMap<>();
        for (UUID id : steps.keySet()) {
            visited.put(id, 0);
        }

        for (UUID id : steps.keySet()) {
            if (visited.get(id) == 0) {
                if (hasCycleDfs(id, steps, visited)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasCycleDfs(UUID id, Map<UUID, DagStep> steps, Map<UUID, Integer> visited) {
        visited.put(id, 1); // visiting

        DagStep step = steps.get(id);
        if (step != null && step.dependsOn() != null) {
            for (UUID depId : step.dependsOn()) {
                if (!steps.containsKey(depId)) {
                    continue;
                }
                Integer state = visited.get(depId);
                if (state == null) continue;
                if (state == 1) {
                    return true; // cycle found
                }
                if (state == 0) {
                    if (hasCycleDfs(depId, steps, visited)) {
                        return true;
                    }
                }
            }
        }

        visited.put(id, 2); // visited
        return false;
    }
}
