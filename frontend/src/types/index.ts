export type StepStatus =
  | "PENDING"
  | "BLOCKED"
  | "ASSIGNED"
  | "IN_PROGRESS"
  | "DONE"
  | "FAILED";

export type BatchStatus = "PENDING" | "RUNNING" | "DONE" | "FAILED";

export type RaftRole = "LEADER" | "FOLLOWER" | "CANDIDATE";

export interface ClusterStatus {
  enabled: boolean;
  nodeId?: string;
  role?: RaftRole;
  term?: number;
  epoch?: number;
  isLeader?: boolean;
}

export interface Metrics {
  batchesCreated: number;
  stepsCompleted: number;
  stepsReassigned: number;
  stepsRerun: number;
}

export interface Batch {
  batch_id: string;
  idempotency_key: string | null;
  status: BatchStatus;
  total_documents: number;
  priority: number;
  created_at: string;
}

export interface Step {
  step_id: string;
  document_id: string;
  status: StepStatus;
  retry_count: number;
  assigned_worker_id: string | null;
  leader_epoch: number;
  updated_at: string;
}

export interface CircuitBreakerStatus {
  state: "CLOSED" | "OPEN" | "HALF_OPEN";
  consecutiveFailures: number;
}

export interface FaultInjectionResponse {
  status: string;
  failureMode: boolean;
  failureDelayMs: number;
}

export interface CreateBatchRequest {
  idempotencyKey?: string;
  documents?: string[];
  priority?: number;
}

export interface DagStepInput {
  name: string;
  documentId: string;
  dependsOn?: string[];
}

export interface CreateDagBatchRequest {
  idempotencyKey?: string;
  priority?: number;
  steps: DagStepInput[];
}