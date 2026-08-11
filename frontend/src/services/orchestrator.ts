import type {
  Batch,
  CircuitBreakerStatus,
  ClusterStatus,
  CreateBatchRequest,
  CreateDagBatchRequest,
  FaultInjectionResponse,
  Metrics,
  Step,
} from "../types";

const PORTS = [8081, 8082, 8083];
const BASE = "/api";

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, options);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed: ${res.status} ${res.statusText} — ${text}`);
  }
  return res.json() as Promise<T>;
}

// Find the current Raft leader port by querying all orchestrators
async function findLeaderPort(): Promise<number> {
  const results = await Promise.allSettled(
    PORTS.map((port) =>
      fetch(`http://localhost:${port}/api/cluster/status`).then(
        (r) => r.json() as Promise<ClusterStatus>
      )
    )
  );
  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    if (r.status === "fulfilled" && r.value.isLeader) {
      return PORTS[i];
    }
  }
  // Fallback: first responding port
  for (let i = 0; i < results.length; i++) {
    if (results[i].status === "fulfilled") return PORTS[i];
  }
  return PORTS[0];
}

export const orchestratorApi = {
  getClusterStatus: () => {
    // Try each orchestrator until one responds
    return Promise.any(
      PORTS.map((port) =>
        fetch(`http://localhost:${port}/api/cluster/status`).then(
          (r) => r.json() as Promise<ClusterStatus>
        )
      )
    ).catch(() => ({ enabled: false } as ClusterStatus));
  },

  getMetrics: () =>
    request<Metrics>(`${BASE}/metrics`).catch(() => ({
      batchesCreated: 0,
      stepsCompleted: 0,
      stepsReassigned: 0,
      stepsRerun: 0,
    })),

  getBatches: () => request<Batch[]>(`${BASE}/batches`),

  createBatch: async (req: CreateBatchRequest) => {
    const port = await findLeaderPort();
    return request<{ batchId: string; totalDocuments: number; status: string }>(
      `http://localhost:${port}/api/batches`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      }
    );
  },

  createDagBatch: async (req: CreateDagBatchRequest) => {
    const port = await findLeaderPort();
    return request<{ batchId: string; totalDocuments: number; status: string }>(
      `http://localhost:${port}/api/batches/dag`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      }
    );
  },

  getBatchSteps: (batchId: string) =>
    request<Step[]>(`${BASE}/batches/${batchId}/steps`),
};

export const workerApi = {
  getCircuitBreakerStatus: (port: number = 8091) =>
    fetch(`http://localhost:${port}/worker/status/circuit-breaker`)
      .then((r) => r.json() as Promise<CircuitBreakerStatus>)
      .catch(() => ({ state: "CLOSED", consecutiveFailures: 0 }) as CircuitBreakerStatus),

  configureFaultInjection: (enabled: boolean, delayMs: number, port: number = 8091) =>
    fetch(`http://localhost:${port}/worker/status/fault-injection`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled, delayMs }),
    }).then((r) => r.json() as Promise<FaultInjectionResponse>),
};