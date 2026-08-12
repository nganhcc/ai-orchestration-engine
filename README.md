# Distributed AI Job Orchestration Engine

Hệ thống điều phối tác vụ AI phân tán (AI Job Orchestration Engine) hỗ trợ lập lịch đồ thị tác vụ (DAG), điều phối theo độ ưu tiên (Deficit Round Robin - DRR), tự phục hồi (Fault Tolerance) sử dụng thuật toán đồng thuận Raft tự triển khai, tích hợp cơ chế ngắt mạch (Circuit Breaker) và giao tiếp bất đồng bộ qua Apache Kafka.

---

## 🏗️ Kiến Trúc Hệ Thống

```
                      ┌───────────────────────────────────────────────┐
                      │          Raft Cluster (3 Orchestrators)       │
                      │  orchestrator-a ◄───Netty TCP───► b/c         │
                      │         │ (Consensus: AppendEntries/Snapshot) │
                      │         ▼ (Leader only)                       │
                      └─────────┬─────────────────────────────────────┘
                                │
                                │ Topic: orchestrator-assign-step
                                ▼
                      ┌──────────────────┐
                      │ Worker Cluster   │ ◄─── REST: Fault Injection
                      │ worker-1 / 2     │
                      └─────────┬────────┘
                                │
                                │ Topic: worker-step-result
                                ▼
                      ┌──────────────────┐
                      │  PostgreSQL 16   │ ◄─── Outbox Poller
                      │ (State & Outbox) │ ───► Redis (Cache Invalidation)
                      └──────────────────┘
```

---

## 🛠️ Công Nghệ

| Layer | Công nghệ |
|-------|-----------|
| **Ngôn ngữ** | Java 21 |
| **Build** | Gradle 9.6.1 Kotlin DSL |
| **Web Framework** | Spring Boot 3.x (orchestrator, worker) |
| **Consensus** | Custom Raft (`raft-core`) |
| **Network** | Netty 4.1.115 (Raft RPC) |
| **Messaging** | Apache Kafka 3.7.0 KRaft (orchestrator ↔ worker) |
| **Database** | PostgreSQL 16 + Flyway |
| **Cache** | Redis 7 (pub/sub cache invalidation) |
| **Frontend** | React 18 + Vite 5 + TypeScript |
| **Container** | Docker Compose |

---

## 📦 Cấu Trúc Module

| Module | Mô tả |
|--------|-------|
| **`raft-core`** | Thuật toán Raft thuần logic: election, log replication, snapshot, compaction. Không phụ thuộc network hay DB. |
| **`rpc-transport`** | Netty TCP wire protocol nhị phân (Length-Prefixed Big-Endian). Dùng cho Raft RPC nội bộ. |
| **`orchestrator`** | Spring Boot coordinator: REST API, Raft cluster bootstrap, DAG/DRR scheduling, Kafka, PostgreSQL persistence, outbox pattern. |
| **`worker`** | Spring Boot executor: Kafka consumer/producer, mock LLM client, circuit breaker, fault injection REST API. |
| **`common`** | Shared DTOs: `AssignStepEvent`, `StepResultEvent`. |
| **`frontend`** | Dashboard React: metrics, cluster status, batch/DAG visualization, worker monitoring, chaos lab. |

---

## 🔑 Quyết Định Thiết Kế Quan Trọng

### 1. Raft Consensus & Fencing Token
- **Custom Raft**: Tự xây dựng hoàn toàn, không dùng thư viện sẵn, để làm chủ quá trình đồng thuận.
- **Epoch = Fencing Token**: Leader đắc cử set `epoch = currentTerm`. Tất cả write path (DB CAS, Kafka messages) đều kèm epoch. Worker/DB reject nếu `epoch < highestEpochSeen` → ngăn split-brain từ leader cũ bị partition.
- **Snapshot Compaction**: `compactUpTo()` + `InstallSnapshot` RPC. Snapshot data = `byte[0]` (rỗng) vì state machine thật ở PostgreSQL — chỉ gửi `lastIncludedIndex/Term` để trimming log và follower catch-up.
- **Durable Log**: `RaftLogStore` strategy pattern — `NO_OP` (in-memory) mặc định, `JdbcRaftLogStore` persist xuống PostgreSQL (`raft_log`, `raft_snapshot`, `raft_meta`). Restore trước khi election bắt đầu.

### 2. Kafka làm Broker Trung Tâm
- Orchestrator ↔ Worker giao tiếp qua **Apache Kafka** (topics: `orchestrator-assign-step`, `worker-step-result`).
- Worker scale out/in dễ dàng: thêm service vào `docker-compose.yml`, Kafka consumer group tự phân phối partition.
- Raft consensus vẫn giữ Netty TCP trực tiếp để giữ độ trễ thấp.

### 3. DAG Scheduling + DRR Priority
- **DAG**: Step có `depends_on` được lưu `BLOCKED`. Khi step DONE → `unblockDependents()` kiểm tra tất cả dependency đã DONE → `BLOCKED → PENDING`. Cycle detection bằng DFS topological sort trước khi lưu.
- **DRR**: `PriorityScheduler` duy trì per-batch `deficit counter`. Quantum = `11 - priority`. StepDispatcher chỉ dispatch batch có deficit > 0, tránh starvation.

### 4. Reliability Patterns
- **Outbox Pattern**: `StepService.markStepDone()` insert `outbox` trong cùng transaction với step status update. `OutboxPublisher` poll và publish sang Redis pub/sub.
- **Stale Step Reaping**: `StepReaper` mỗi 5s reset steps `ASSIGNED/IN_PROGRESS` quá hạn (10s) về `PENDING` để dispatch lại.
- **Circuit Breaker**: Thread-safe state machine `CLOSED → OPEN → HALF_OPEN`. Khi OPEN, worker drop step → orchestrator reaper reclaim.
- **Idempotency Key**: `POST /api/batches` hỗ trợ header `Idempotency-Key`. Trùng key → trả về `batchId` cũ.
- **Leader-Only Execution**: `StepDispatcher`, `StepReaper`, mutation endpoints đều `if (!isLeader()) return;`.

### 5. Pure Logic First
- `RaftMessageHandler` là hàm thuần `handleRequestVote(state, req) → response`, test được không cần network.
- Chỉ sau Phase 1.1–1.3 pass test mới gắn Netty ở Phase 2.

---

## 🚀 Hướng Dẫn Chạy

### Yêu cầu
- Java 21
- Gradle 9.x
- Docker & Docker Compose
- Node.js 18+ (frontend)

### 1. Build

```bash
./gradlew build
```

### 2. Start Cluster (Docker Compose)

```bash
docker compose up -d --build
```

Services:
| Service | Port | Mô tả |
|---------|------|-------|
| `postgres` | 5432 | PostgreSQL 16 |
| `redis` | 6379 | Redis 7 |
| `kafka` | 9092 | Apache Kafka 3.7.0 KRaft |
| `orchestrator-a` | 8081, 7001 | Raft node A |
| `orchestrator-b` | 8082, 7002 | Raft node B |
| `orchestrator-c` | 8083, 7003 | Raft node C |
| `worker` | 8091 | Worker 1 |
| `worker-2` | 8092 | Worker 2 |

### 3. Chạy Frontend

```bash
cd frontend
npm install
npm run dev
```

Mở trình duyệt: **http://localhost:5173/**

Frontend proxy:
- `/api` → `http://localhost:8081` (orchestrator leader)
- `/worker` → `http://localhost:8091` (worker-1)

Để expose ra LAN:
```bash
cd frontend && npm run dev -- --host
```

### 4. Demo Chaos Tự Động

```bash
bash demo_chaos.sh
```

Script tự động thực hiện:
1. Phát hiện leader hiện tại
2. Submit DAG batch 4 steps (OCR → Extract → Translate → Validate)
3. Inject fault vào worker-1 → kích hoạt Circuit Breaker
4. Network partition leader (disconnect)
5. Cluster bầu leader mới, epoch tăng
6. Reconnect leader cũ → verify epoch fencing
7. Tắt fault injection → worker hồi phục → batch tự hoàn tất

---

## 🌐 Luồng Xử Lý Request

### Client → Leader Discovery

Client có thể gọi vào **bất kỳ** orchestrator node (`8081`, `8082`, `8083`):

```bash
POST http://localhost:8081/api/batches/dag
POST http://localhost:8082/api/batches/dag
POST http://localhost:8083/api/batches/dag
```

Nếu node không phải leader → trả về **HTTP 307 Temporary Redirect**. Client retry → đến đúng leader.

Frontend tự động discover leader qua `GET /api/cluster/status` trên tất cả node, tìm node có `"isLeader": true`.

### Leader Xử Lý Batch

```
Client → POST /api/batches/dag → Leader
                              ↓
                    isLeader()? epoch = currentTerm
                   /                  \
               NO                     YES
                |                       |
        307 Redirect             Cycle Detection (DFS)
                |                       |
         Client retry              INSERT batch_job + job_step
                |                       |
         (đến đúng leader)       201 Created (batchId)
```

### Leader Tự Động Dispatch Steps

```
StepDispatcher (mỗi 500ms, leader only)
  → Query PENDING steps + DRR scheduling
  → markStepAssigned(epoch CAS)
  → Kafka publish → orchestrator-assign-step
```

### Worker Xử Lý Step

```
Worker ← Kafka consume AssignStepEvent
  → Epoch fencing check
  → Dedup (in-memory + DB)
  → CircuitBreaker.execute(MockLlmClient)
  → Kafka publish → worker-step-result
```

### Leader Nhận Kết Quả

```
Leader ← Kafka consume StepResultEvent
  → StepService.markStepDone(epoch CAS)
  → INSERT outbox (cùng transaction)
  → DAG unblock dependents
  → Check batch completion
```

### Reliability Loop

```
OutboxPublisher (mỗi 2s)
  → SELECT unpublished outbox
  → Redis pub/sub step-done-events
  → UPDATE outbox SET published=true

StepReaper (mỗi 5s)
  → Reset stale IN_PROGRESS/ASSIGNED → PENDING
  → Increment stepsReassigned metric
```

---

## 📡 API Reference

### Orchestrator (Leader only cho mutations)

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `POST` | `/api/batches` | Tạo batch đơn giản |
| `POST` | `/api/batches/dag` | Tạo batch DAG |
| `GET` | `/api/batches` | Danh sách batches |
| `GET` | `/api/batches/{batchId}/steps` | Chi tiết steps của batch |
| `GET` | `/api/cluster/status` | Trạng thái Raft cluster |
| `GET` | `/api/metrics` | Metrics vận hành |

**Cluster Status Response:**
```json
{
  "enabled": true,
  "nodeId": "orchestrator-a",
  "role": "LEADER",
  "term": 5,
  "epoch": 5,
  "isLeader": true
}
```

**Metrics Response:**
```json
{
  "batchesCreated": 10,
  "stepsCompleted": 40,
  "stepsReassigned": 2,
  "stepsRerun": 0
}
```

### Worker

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/worker/status/circuit-breaker` | Trạng thái circuit breaker |
| `POST` | `/worker/status/fault-injection` | Bật/tắt fault injection |

---

## 🖥️ Frontend Dashboard

| Trang | Route | Mô tả |
|-------|-------|-------|
| **Dashboard** | `/` | Metrics realtime + Raft cluster status (node, role, term, epoch) |
| **Batches** | `/batches` | Danh sách batches, tạo batch đơn giản hoặc DAG |
| **Batch Detail** | `/batches/:batchId` | React Flow DAG visualization + danh sách steps theo trạng thái |
| **Workers** | `/workers` | Giám sát circuit breaker (CLOSED/OPEN/HALF_OPEN), điều khiển fault injection |
| **Chaos Lab** | `/chaos` | Kịch bản chaos demo, event log |

---

## 🧪 Testing

```bash
# Test Raft logic thuần (no network)
./gradlew :raft-core:test

# Test wire protocol
./gradlew :rpc-transport:test

# Test orchestrator + integration
./gradlew :orchestrator:test

# Test worker
./gradlew :worker:test

# Test 1 class cụ thể
./gradlew :raft-core:test --tests "RaftLogTest"
./gradlew :orchestrator:test --tests "RaftNettyClusterIntegrationTest"
```

---

## ⚙️ Cấu Hình

### Environment Variables (Orchestrator)

| Biến | Mô tả | Mặc định |
|------|-------|----------|
| `RAFT_NODE_ID` | ID node Raft | bắt buộc |
| `RAFT_PORT` | Port Raft TCP | `7000` |
| `RAFT_BIND_HOST` | Bind address | `0.0.0.0` |
| `RAFT_PEERS` | Peers map (`id=host:port,...`) | bắt buộc |
| `ORCHESTRATOR_STEPSTALENESTHRESHOLDSECONDS` | Thời gian stale trước khi reape | `30` |

### Environment Variables (Worker)

| Biến | Mô tả | Mặc định |
|------|-------|----------|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | Kafka broker | `localhost:9092` |
| `SPRING_DATASOURCE_URL` | PostgreSQL JDBC | `jdbc:postgresql://postgres:5432/orchestration` |
| `SERVER_PORT` | HTTP server port | `8091` |

---

## 📊 Database Schema

```sql
CREATE TABLE batch_job (
    batch_id UUID PRIMARY KEY,
    idempotency_key VARCHAR(128) UNIQUE,
    status VARCHAR(20) NOT NULL,              -- PENDING, RUNNING, DONE, FAILED
    total_documents INT NOT NULL,
    priority INT NOT NULL DEFAULT 5,          -- 1 (cao nhất) -> 10 (thấp nhất)
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE job_step (
    step_id UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES batch_job(batch_id),
    document_id VARCHAR(255) NOT NULL,
    depends_on UUID[],                        -- Chứa ID các step cần DONE trước
    status VARCHAR(20) NOT NULL,              -- PENDING, BLOCKED, ASSIGNED, IN_PROGRESS, DONE, FAILED
    assigned_worker_id VARCHAR(64),
    leader_epoch BIGINT NOT NULL,             -- Fencing check
    result JSONB,
    retry_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE raft_log (
    node_id VARCHAR(64) NOT NULL,
    log_index BIGINT NOT NULL,
    term BIGINT NOT NULL,
    command BYTEA NOT NULL,
    PRIMARY KEY (node_id, log_index)
);

CREATE TABLE raft_snapshot (
    node_id VARCHAR(64) NOT NULL,
    last_included_index BIGINT NOT NULL,
    last_included_term BIGINT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id)
);

CREATE TABLE raft_meta (
    node_id VARCHAR(64) PRIMARY KEY,
    current_term BIGINT NOT NULL,
    voted_for VARCHAR(64),
    commit_index BIGINT NOT NULL,
    last_applied BIGINT NOT NULL
);

CREATE TABLE outbox (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE worker_step_status (
    step_id UUID PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    result JSONB,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
```

---

## 🧩 Wire Protocol (Netty Frame)

Mọi gói tin qua mạng đều dùng định dạng Length-Prefixed Big-Endian:

```
+---------------------------+----------------+----------------+-------------------+--------+
| totalLength (int, 4 B)    | messageType(1B)| requestId(8B)  | epoch (8B)        | payload|
+---------------------------+----------------+----------------+-------------------+--------+
```

- `totalLength`: `1 + 8 + 8 + N` bytes (không tính 4 byte của chính nó)
- `maxFrameLength`: 1MB

**Message Types:**
| Type | Value | Mô tả |
|------|-------|-------|
| `RAFT_REQUEST_VOTE` | `0x10` | Raft election |
| `RAFT_APPEND_ENTRIES` | `0x12` | Raft log replication |
| `RAFT_INSTALL_SNAPSHOT` | `0x14` | Raft snapshot transfer |
| `ASSIGN_STEP` | `0x01` | Orchestrator → Worker |
| `STEP_RESULT` | `0x06` | Worker → Orchestrator |
| `HEARTBEAT` | `0x03` | Keep-alive |
| `ACK` | `0x04` | Heartbeat response |
| `STALE_LEADER_REJECT` | `0x05` | Epoch fencing reject |

---

## 🐛 Troubleshooting

### Port đã được sử dụng
```bash
# Kiểm tra port
lsof -i :8081 -i :8082 -i :8083 -i :8091 -i :8092 -i :5432 -i :6379 -i :9092

# Dừng container cũ
docker compose down
```

### Container không start được
```bash
# Xem logs
docker compose logs orchestrator-a
docker compose logs worker

# Rebuild từ đầu
docker compose down
./gradlew build -x test
docker compose up -d --build
```

### Frontend kết nối lỗi
- Đảm bảo backend đã chạy (`docker compose ps`)
- Kiểm tra CORS: frontend chạy ở `5173`, backend allow origin này
- Nếu chạy frontend với `--host`, cần cập nhật CORS config

### Kafka message bị lặp
- Worker có dedup in-memory + DB (`worker_step_status`)
- Orchestrator có idempotency key + epoch CAS

---

## 📈 Metrics Đo Đạc

Sau khi chạy `demo_chaos.sh`, quan sát:

```bash
curl http://localhost:8081/api/metrics | jq
```

| Metric | Mục tiêu | Mô tả |
|--------|----------|-------|
| `stepsRerun` | 0% | Step bị chạy lại do worker crash |
| `stepsReassigned` | >0 | Step được reclaim bởi StepReaper |
| `stepsCompleted` | = total steps | Tổng steps đã hoàn thành |
| `batchesCreated` | = submitted | Tổng batch đã tạo |

---
