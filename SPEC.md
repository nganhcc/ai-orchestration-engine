# Distributed AI Job Orchestration Engine — Đặc tả kỹ thuật

## 1. Tổng quan

### 1.1 Bài toán

Hệ thống xử lý hàng loạt tài liệu (PDF/scan) cho doanh nghiệp vừa/nhỏ (kế toán, luật, bảo hiểm), trích xuất thông tin bằng LLM qua **pipeline nhiều bước** (OCR → extract → validate). Yêu cầu:

- Xử lý song song hàng nghìn tài liệu, mỗi tài liệu là một DAG các bước có dependency.
- Không mất tiến trình, không gọi lại LLM đã xử lý xong khi node crash giữa chừng.
- Không có single point of failure ở tầng điều phối, và **không bị hai leader cùng ghi dữ liệu** khi mạng bị partition (không chỉ khi node chết hẳn).
- Giảm chi phí gọi LLM qua caching + circuit breaker khi provider degrade.
- Chịu tải tăng đột biến qua backpressure, không đơn giản retry-storm.

### 1.2 Mục tiêu kỹ thuật

| Mục tiêu | Kỹ thuật |
|---|---|
| Không có leader đơn lẻ gây SPOF | Raft consensus, leader election |
| Không bị split-brain khi network partition (không chỉ khi crash) | Fencing token / epoch trên mọi RPC và mọi write xuống storage |
| Giao tiếp orchestrator ↔ worker (low-level) | Custom RPC layer, tự marshal, exactly-once semantics |
| Giao tiếp orchestrator ↔ worker (scale-out) | Kafka làm data-plane transport thay thế, tự rebalance + backpressure qua consumer lag |
| Không mất/không lặp công việc khi crash | Write-ahead log (WAL), checkpoint/resume, log compaction |
| Ghi đồng thời Postgres + Redis không bị "dual write" hỏng | Outbox pattern (không dùng 2PC blocking) |
| Pipeline nhiều bước có dependency/document | DAG scheduling trong state machine |
| Công bằng giữa nhiều batch/khách hàng | Priority / weighted fair scheduling |
| Giảm chi phí gọi LLM | Cache layer (LRU + invalidation) |
| Chịu lỗi provider bên ngoài | Circuit breaker + retry với backoff |
| Chịu lỗi mạng/node | Chaos testing, timeout, retry với backoff |
| Truy vết request xuyên hệ thống | traceId lan truyền qua custom RPC + Kafka headers |
| Analytics thời gian thực | Spark Structured Streaming trên topic Kafka |

### 1.3 Ngoài phạm vi

- ML router phân loại độ khó document.
- Semantic cache (vector similarity).
- UI phức tạp (workflow builder kéo-thả) — chỉ làm dashboard giám sát.
- Full OpenTelemetry collector — chỉ propagate `traceId` + log structured, chưa cần collector/backend riêng.

---

## 2. Tech stack

| Thành phần | Công nghệ |
|---|---|
| Ngôn ngữ | Java 21 |
| Framework | Spring Boot 3.x |
| RPC transport (low-level) | Netty (TCP), tự định nghĩa wire protocol — **không** dùng gRPC/Thrift |
| RPC transport (scale-out) | Apache Kafka — topic `step-assignments`, `step-results` |
| Serialization | Tự viết binary protocol (length-prefixed frame + custom encode/decode) cho RPC layer; Avro/JSON cho Kafka payload |
| Metadata & WAL storage | PostgreSQL 16 (kèm bảng `outbox`) |
| Cache | Redis 7 |
| Raft implementation | Tự cài đặt core (leader election + log replication + snapshot) trong module `raft-core` |
| Analytics streaming | Spark Structured Streaming (đọc từ Kafka `step-results`) |
| Build | Gradle (multi-module) |
| Containerization | Docker Compose (orchestrator x3, worker xN, Postgres, Redis, Kafka x3 broker, Spark) |
| Chaos testing | Script bash: `docker network disconnect` (partition thật, không chỉ `docker kill`), `docker kill`, `tc netem` (thêm latency/packet loss) |
| Dashboard | Thymeleaf + SSE, hoặc React tách riêng |

---

## 3. Kiến trúc tổng thể

```
                    ┌─────────────────────────┐
                    │      Client / API         │
                    │  (submit batch, Idempotency-Key) │
                    └────────────┬─────────────┘
                                 │ HTTP (Spring Boot REST)
                                 ▼
                    ┌─────────────────────────┐
                    │   Orchestrator Cluster     │
                    │  (3 node, Raft consensus)  │
                    │  epoch/fencing token tăng   │
                    │  mỗi lần bầu leader mới     │
                    │  Node A (Leader, epoch=7)   │
                    │  Node B (Follower)          │
                    │  Node C (Follower)          │
                    └──────┬──────────────┬───────┘
                    Custom RPC (Netty)   Kafka produce
                    (low-level)          (scale-out)
                           │                  │
                    ┌──────┼──────┐    ┌──────┼───────┐
                    ▼      ▼      ▼    ▼      ▼       ▼
              ┌──────────┐┌──────────┐┌──────────┐
              │ Worker 1 ││ Worker 2 ││ Worker N │  (consumer group nếu dùng Kafka)
              └────┬─────┘└────┬─────┘└────┬─────┘
                   │  circuit breaker + cache-aside │
                   ▼            ▼             ▼
              ┌─────────────────────────────────┐
              │   LLM Provider (OpenAI/Anthropic) │
              └─────────────────────────────────┘

  Storage layer:
  ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
  │  PostgreSQL     │   │     Redis       │   │  Kafka topics   │
  │  - job/step      │   │  - cache layer  │   │  step-assignments│
  │  - outbox         │   │  - rate counter │   │  step-results    │
  │  - Raft log+snap  │   └───────────────┘   └────────┬────────┘
  └───────────────┘                                    │
                                                         ▼
                                              ┌───────────────────┐
                                              │ Spark Structured    │
                                              │ Streaming (metrics) │
                                              └───────────────────┘
```

---

## 4. Module Raft Consensus (`raft-core`)

### 4.1 Trạng thái node

```java
enum NodeState { FOLLOWER, CANDIDATE, LEADER }

class RaftState {
    long currentTerm;
    long epoch;              // = currentTerm khi trở thành leader; gắn vào MỌI outbound RPC/write
    String votedFor;
    List<LogEntry> log;
    long commitIndex;
    long lastApplied;
    long lastSnapshotIndex;  // index đã được gói vào snapshot gần nhất
    long lastSnapshotTerm;

    Map<String, Long> nextIndex;
    Map<String, Long> matchIndex;
}

class LogEntry {
    long term;
    long index;
    byte[] command;
}
```

### 4.2 RPC nội bộ giữa các Raft node

| RPC | Request | Response |
|---|---|---|
| `RequestVote` | `term, candidateId, lastLogIndex, lastLogTerm` | `term, voteGranted` |
| `AppendEntries` | `term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit` | `term, success, matchIndex` |
| `InstallSnapshot` | `term, lastIncludedIndex, lastIncludedTerm, data` | `term` |

### 4.3 Timing

| Tham số | Giá trị đề xuất |
|---|---|
| Election timeout | random(150ms, 300ms) |
| Heartbeat interval | 50ms |
| RPC timeout | 100ms |
| Snapshot trigger | mỗi 10,000 log entries hoặc mỗi 5 phút, chọn ngưỡng nào tới trước |

### 4.4 Log compaction / Snapshot

Với batch 5,000+ document, mỗi `MarkStepDone` là một log entry → log phình rất nhanh, restart node phải replay toàn bộ → chậm.

- Định kỳ, leader chụp snapshot: serialize toàn bộ in-memory job table (state machine hiện tại) thành `data`, ghi kèm `lastIncludedIndex/Term`.
- Log entries có `index <= lastIncludedIndex` bị xoá khỏi bảng `raft_log`.
- Follower tụt hậu quá xa (nextIndex < lastSnapshotIndex của leader) nhận `InstallSnapshot` thay vì replay từng `AppendEntries`.
- Khi node restart: load snapshot gần nhất trước, rồi mới replay log entries còn lại phía sau `lastSnapshotIndex`.

### 4.5 Fencing token / chống split-brain

Vấn đề: leader cũ có thể chỉ bị **network partition** chứ chưa chết — nó vẫn nghĩ mình là leader và tiếp tục gửi `AssignStep`/ghi Postgres, trong khi phần cluster còn lại đã bầu leader mới.

Giải pháp:

- Mỗi lần một node thắng election, `epoch := currentTerm` mới, và **mọi** RPC ra ngoài (tới worker, tới Postgres write, tới Kafka produce) đều đính kèm `epoch` này.
- Worker lưu `highestEpochSeen`. Nếu nhận RPC có `epoch < highestEpochSeen` → từ chối (`STALE_LEADER`), không thực thi.
- Ghi Postgres cũng kiểm tra epoch: cột `leader_epoch` trên `batch_job`/`job_step`, update có điều kiện `WHERE leader_epoch <= :epoch` (compare-and-swap ở tầng SQL).
- Kết quả: leader cũ dù còn "tưởng mình là leader" cũng không thể tạo hiệu ứng phụ nào lên state — đây là kỹ thuật chuẩn trong các hệ Raft/Paxos thực tế (tương tự "generation clock" trong GFS/Chubby).

### 4.6 State machine áp dụng lên log

```java
sealed interface JobCommand {
    record SubmitBatch(String batchId, List<String> documentIds) implements JobCommand {}
    record SubmitDagStep(String jobId, String stepId, List<String> dependsOn, int priority) implements JobCommand {}
    record AssignStep(String jobId, String stepId, String workerId, long epoch) implements JobCommand {}
    record MarkStepDone(String jobId, String stepId, byte[] result) implements JobCommand {}
    record MarkStepFailed(String jobId, String stepId, String reason) implements JobCommand {}
}
```

### 4.7 Leader election flow

Follower timeout → candidate → `RequestVote` → majority → leader; heartbeat rỗng giữ quyền leader. Ngay khi trở thành leader, node **tăng epoch và broadcast epoch mới** tới toàn bộ worker đã biết trước khi bắt đầu assign step.

---

## 5. Custom RPC Layer (Orchestrator ↔ Worker — low-level transport)

### 5.1 Wire protocol

Frame format (length-prefixed, big-endian):

```
+------------+------------+------------+------------+------------------+
| 4 bytes    | 1 byte     | 8 bytes    | 8 bytes    | N bytes          |
| totalLength| messageType| requestId  | epoch      | payload          |
+------------+------------+------------+------------+------------------+
```

- `messageType`: `0x01=ASSIGN_STEP`, `0x02=STEP_RESULT`, `0x03=HEARTBEAT`, `0x04=ACK`, `0x05=STALE_LEADER_REJECT`
- `epoch` (8 bytes): fencing token — worker so với `highestEpochSeen`, từ chối nếu cũ hơn.
- `traceId` (đặt trong payload field đầu tiên của mọi message type): 16 bytes UUID, log xuyên suốt orchestrator → worker → LLM call để correlate log theo request.
- **Quy ước `totalLength` (chốt khi triển khai Phase 2, spec gốc để ngỏ):** `totalLength` = số byte của phần *sau* nó, tức `messageType(1) + requestId(8) + epoch(8) + payload(N)` — **không** tính 4 byte của chính field `totalLength`. Ví dụ `HEARTBEAT` với payload rỗng: `totalLength = 1+8+8+0 = 17`, tổng frame trên wire = `4+17 = 21` byte. Quy ước này ánh xạ trực tiếp sang tham số của `LengthFieldBasedFrameDecoder` (Netty): `lengthFieldOffset=0, lengthFieldLength=4, lengthAdjustment=0, initialBytesToStrip=4`.

```
ASSIGN_STEP payload:
  [16 bytes traceId]
  [2 bytes jobIdLen][jobId bytes]
  [2 bytes stepIdLen][stepId bytes]
  [4 bytes payloadLen][payload bytes]
```

**Giới hạn chống DoS (bổ sung khi triển khai):** `LengthFieldBasedFrameDecoder` phía nhận cấu hình `maxFrameLength = 1MB` — nếu 1 client khai `totalLength` giả mạo (ví dụ cực lớn), decoder ném `TooLongFrameException` thay vì cố cấp phát buffer khổng lồ chờ đủ byte. Ngưỡng 1MB có thể điều chỉnh sau khi biết kích thước payload LLM thực tế lớn nhất ở Phase 5.

### 5.2 Semantics: exactly-once xử lý step

Dedup table `stepId -> {IN_PROGRESS, DONE}` ở worker (in-memory + Postgres-backed), trả kết quả cũ nếu nhận lại `stepId` đã `DONE`. Worker kiểm tra `epoch` **trước** khi tra dedup table — request có epoch cũ bị từ chối ngay, không chạm state.

### 5.3 Backpressure

- Worker báo `freeSlots` (capacity còn trống) trong mỗi `HEARTBEAT`.
- Orchestrator duy trì `Map<workerId, availableCredit>`; chỉ gửi `ASSIGN_STEP` khi `availableCredit > 0`, giảm credit khi gửi, tăng lại khi nhận `ACK`/`STEP_RESULT`.
- Tương tự cơ chế credit-based flow control của TCP sliding window — tránh retry-storm khi worker quá tải thay vì để orchestrator dội step liên tục vào worker đã nghẽn.

### 5.4 Retry & timeout

| Tham số | Giá trị |
|---|---|
| RPC timeout (orchestrator chờ ACK) | 5s |
| Retry tối đa | 3 lần, backoff: 1s, 3s, 9s |
| Sau khi hết retry | Orchestrator coi worker là dead, reassign step cho worker khác |

---

## 6. Kafka Transport (data plane cho scale-out)

Custom RPC (mục 5) vẫn giữ nguyên làm lựa chọn "low-level" — module `rpc-transport`. Kafka là lựa chọn thứ hai, module `kafka-transport`, bật qua config `orchestration.transport=kafka|rpc`.

### 6.1 Topic design

| Topic | Key | Value | Partition theo |
|---|---|---|---|
| `step-assignments` | `jobId` | `AssignStep` (Avro), kèm header `epoch`, `traceId` | `jobId` — đảm bảo các step cùng job vào cùng partition, giữ thứ tự |
| `step-results` | `jobId` | `StepResult` (Avro) | `jobId` |

### 6.2 Vì sao Kafka giải quyết backpressure "miễn phí"

- Consumer group của worker tự rebalance khi thêm/bớt worker — không cần logic assign thủ công.
- **Consumer lag** = tín hiệu trực tiếp "worker đang quá tải" — orchestrator (hoặc một control loop riêng) theo dõi lag, tạm dừng produce thêm nếu lag vượt ngưỡng, thay vì tự cài credit-based flow control như mục 5.3.
- Worker crash và restart: đọc lại từ offset đã commit — gần như có WAL miễn phí ở tầng transport, bổ sung cho WAL ở tầng Postgres (mục 7).

### 6.3 Idempotency khi dùng Kafka

Kafka tự nó chỉ đảm bảo at-least-once (trừ khi bật transactional producer, phức tạp hơn cần thiết cho phạm vi này) → vẫn giữ dedup table ở worker y như mục 5.2, `stepId` làm khoá idempotency.

---

## 7. Write-Ahead Log, Checkpoint/Resume & Outbox Pattern

### 7.1 Schema PostgreSQL

```sql
CREATE TABLE batch_job (
    batch_id UUID PRIMARY KEY,
    idempotency_key VARCHAR(128) UNIQUE,      -- chống submit trùng do client retry
    status VARCHAR(20) NOT NULL,
    total_documents INT NOT NULL,
    priority INT NOT NULL DEFAULT 5,          -- 1 (cao) .. 10 (thấp), cho fair scheduling
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE job_step (
    step_id UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES batch_job(batch_id),
    document_id VARCHAR(255) NOT NULL,
    depends_on UUID[],                        -- DAG dependency, NULL/rỗng nếu là step gốc
    status VARCHAR(20) NOT NULL,              -- PENDING, BLOCKED, ASSIGNED, IN_PROGRESS, DONE, FAILED
    assigned_worker_id VARCHAR(64),
    leader_epoch BIGINT NOT NULL,             -- fencing — update chỉ hợp lệ nếu epoch >= giá trị này
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

CREATE TABLE outbox (                          -- transactional outbox, thay 2PC
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,                -- vd: step_id
    event_type VARCHAR(50) NOT NULL,           -- CACHE_INVALIDATE, STEP_DONE_NOTIFY, ...
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_job_step_batch_status ON job_step(batch_id, status);
CREATE INDEX idx_outbox_unpublished ON outbox(published) WHERE NOT published;
```

### 7.2 Outbox pattern thay cho 2PC

Vấn đề gốc: khi step `DONE`, cần ghi **cả** Postgres (`job_step.status=DONE`) **và** Redis (set cache) **và** (nếu có) publish webhook/Kafka event — ba thao tác này không atomic, dual-write có thể lệch nhau nếu crash giữa chừng.

Cân nhắc và loại bỏ 2PC thật: coordinator-blocking (nếu coordinator chết ở phase 2, participant giữ lock vô thời hạn), lại không tận dụng được Raft đã có sẵn cho consensus. Chọn outbox pattern:

1. Trong **cùng một transaction Postgres**: update `job_step.status = DONE` + insert row vào `outbox` (event `CACHE_INVALIDATE` hoặc `STEP_DONE_NOTIFY`).
2. Một background poller (hoặc chính leader khi apply Raft log) đọc `outbox WHERE NOT published`, publish sang Redis/Kafka, rồi `UPDATE outbox SET published = true`.
3. Nếu crash giữa bước 2, restart sẽ thấy `published = false` → publish lại — an toàn vì thao tác phía sau (invalidate cache, gửi webhook) cần tự thiết kế idempotent (dùng `event_id` làm dedup key).

Kết quả: Postgres write không bao giờ "treo" chờ Redis/Kafka; nhất quán cuối cùng (eventual) giữa hai store nhưng không mất event.

### 7.3 Resume flow sau crash

1. Leader mới (sau election, epoch tăng) load `job_step` với `status IN ('ASSIGNED', 'IN_PROGRESS')` và `updated_at` quá `staleness_threshold` (30s).
2. Với mỗi step: nếu worker cũ còn sống và dedup table báo `DONE` → cập nhật kết quả (kiểm tra `leader_epoch` hợp lệ trước khi ghi), không rerun. Nếu worker chết hoặc dedup chưa xong → reassign.
3. Step `DONE` không bao giờ chạy lại.

---

## 8. DAG Scheduling — Pipeline nhiều bước/document

Thực tế một document không chỉ 1 step mà là chuỗi: `OCR → extract → validate`, có thể rẽ nhánh (extract song song nhiều field-group rồi merge).

- Mỗi `job_step` có `depends_on: UUID[]`.
- Orchestrator chỉ chuyển step từ `BLOCKED` sang `PENDING` (sẵn sàng assign) khi **toàn bộ** step trong `depends_on` đã `DONE` — kiểm tra này chạy trong state machine khi apply `MarkStepDone`, giống topological scheduling trong Spark DAGScheduler/Dryad.
- `SubmitDagStep` command (mục 4.6) cho phép client submit cả cây DAG cùng lúc; orchestrator validate không có cycle trước khi accept (submit batch bị reject với lỗi rõ ràng nếu phát hiện cycle).

---

## 9. Priority / Fair Scheduling giữa nhiều batch

- `batch_job.priority` (1 cao – 10 thấp). Khi assign step cho worker rảnh, orchestrator chọn theo weighted round-robin giữa các batch đang `RUNNING`, không đơn thuần FIFO — tránh batch nộp sau bị đói nếu batch nộp trước rất lớn (5,000 document).
- Đơn giản hoá: dùng thuật toán **deficit round robin** giữa các batch active, không cần scheduler phức tạp kiểu Kubernetes.

---

## 10. Cache Layer & Circuit Breaker

### 10.1 Cache-aside

- Key: `hash(document_content + prompt_template_version)`, TTL 7 ngày, Redis `allkeys-lru`.
- Invalidation tự nhiên khi `prompt_template_version` tăng.

### 10.2 Circuit breaker cho LLM provider

Retry với backoff (mục 5.4/11) xử lý lỗi tạm thời, nhưng nếu provider down hẳn, mọi worker vẫn dội retry liên tục → lãng phí và có nguy cơ bị rate-limit ban. Thêm circuit breaker ở `LlmClient`:

- `CLOSED` (bình thường) → quá N lỗi liên tiếp trong cửa sổ thời gian → `OPEN` (chặn mọi call, trả lỗi ngay, step chuyển `FAILED` tạm thời với reason `PROVIDER_DEGRADED`) → sau `cooldown` chuyển `HALF_OPEN` (cho 1 request thử) → thành công thì `CLOSED` lại, thất bại thì `OPEN` tiếp.
- Step bị `FAILED` do `PROVIDER_DEGRADED` được orchestrator tự động enqueue lại vào một "retry-later queue" thay vì đếm vào `retry_count` thông thường, tránh đốt hết quota retry cho lỗi không phải do step.

---

## 11. Observability

### 11.1 Distributed tracing tối giản

- `traceId` sinh ở client (hoặc orchestrator nếu client không gửi), lan truyền qua: HTTP header → Raft command payload → RPC frame (mục 5.1) hoặc Kafka message header (mục 6.1) → LLM call log.
- Log structured (JSON) tại mọi service, field bắt buộc: `traceId, jobId, stepId, epoch, service, timestamp`.
- Không cần OpenTelemetry collector đầy đủ — chỉ cần correlate log qua `traceId` bằng `grep`/Kibana đơn giản là đủ demo giá trị.

### 11.2 Metric cần theo dõi

- p50/p99 latency RPC round-trip (cả 2 transport).
- Consumer lag theo thời gian (nếu dùng Kafka transport).
- Thời gian mỗi giai đoạn của election (không chỉ tổng failover time).
- Số lần circuit breaker chuyển `OPEN`, thời gian ở trạng thái `OPEN`.
- Queue depth / backpressure credit theo thời gian trong chaos test.

---

## 12. API (REST, expose qua orchestrator leader)

| Method | Endpoint | Mô tả |
|---|---|---|
| POST | `/api/batches` | Submit batch, header `Idempotency-Key` bắt buộc → trả `batchId` (trả lại `batchId` cũ nếu key trùng) |
| POST | `/api/batches/dag` | Submit batch dạng DAG steps (mục 8) |
| GET | `/api/batches/{batchId}` | Trạng thái tổng quan |
| GET | `/api/batches/{batchId}/steps` | Chi tiết từng step, bao gồm `dependsOn` |
| POST | `/api/batches/{batchId}/retry` | Retry step failed |
| GET | `/api/cluster/status` | Leader hiện tại, term, **epoch** |
| GET (SSE) | `/api/batches/{batchId}/stream` | Cập nhật real-time |

Follower nhận request → `HTTP 307 Redirect` tới leader (lấy từ Raft state, kèm epoch hiện tại để client log lại).

---

## 13. Chaos Testing

```bash
#!/bin/bash
# Kịch bản 1: kill leader
LEADER_CONTAINER=$(curl -s localhost:8080/api/cluster/status | jq -r .leaderContainer)
docker kill "$LEADER_CONTAINER"

# Kịch bản 2: network partition, KHÔNG kill process — test fencing token
docker network disconnect ai-net "$LEADER_CONTAINER"
sleep 5
# leader cũ vẫn "sống" và nghĩ mình là leader — verify nó bị reject (STALE_LEADER) khi cố ghi
docker network connect ai-net "$LEADER_CONTAINER"

# Kịch bản 3: thêm latency/packet loss thay vì cắt hẳn
docker exec worker-1 tc qdisc add dev eth0 root netem delay 200ms loss 10%
```

Kịch bản demo mở rộng:

1. Submit batch DAG 5,000 document, priority trộn giữa 2 batch.
2. Ở ~50% tiến trình: (a) kill leader — đo failover; (b) partition leader (không kill) — verify epoch cũ bị reject, không job nào bị 2 leader cùng ghi.
3. Tắt tạm LLM provider (mock lỗi 100%) — quan sát circuit breaker chuyển `OPEN`, step không bị đốt hết retry.
4. Ghi metric: failover time, % step chạy lại (mục tiêu 0%), circuit breaker OPEN duration, cache hit rate.

---

## 14. Dashboard

- **Cluster view**: 3 node, leader hiện tại, term, **epoch**, heartbeat/s.
- **Batch view**: progress bar, DAG visualization đơn giản (step nào blocked/done), nút retry.
- **Cost & cache view**: cache hit/miss, chi phí tiết kiệm — nguồn dữ liệu từ Spark Structured Streaming job (mục 15) thay vì query trực tiếp Postgres, để tách biệt analytics khỏi core write path.

---

## 15. Spark Structured Streaming — Analytics

Không dùng Spark cho core orchestration (sai công cụ ở quy mô vài nghìn document/batch) — chỉ dùng cho **side-channel analytics** trên luồng `step-results` (nếu bật Kafka transport):

```
Kafka topic "step-results"
      │
      ▼
Spark Structured Streaming job
      │  - windowed aggregation (tumbling window 1 phút)
      │  - tính: throughput, cache_hit_rate, cost_saved_estimate theo batch
      ▼
Sink: Postgres bảng "batch_metrics" (đọc bởi dashboard) hoặc trực tiếp SSE
```

Đây là kiến trúc kappa quen thuộc: cùng một luồng sự kiện phục vụ cả xử lý chính (worker consume để làm việc) lẫn phân tích (Spark consume để tổng hợp), không phải ETL batch riêng.

---

## 16. Cấu trúc project (Gradle multi-module)

```
ai-orchestration-engine/
├── raft-core/           # Raft: election, log replication, snapshot, epoch/fencing
├── rpc-transport/        # Netty TCP, wire protocol low-level
├── kafka-transport/       # Kafka producer/consumer, topic config
├── orchestrator/           # REST, JobScheduler (DAG + priority), WalRepository, OutboxPublisher
├── worker/                   # StepExecutor, DedupTable, CacheClient, LlmClient (circuit breaker)
├── analytics/                 # Spark Structured Streaming job
├── dashboard/                   # Thymeleaf + SSE
├── chaos-test/                    # Bash scripts (kill, partition, netem) + docker-compose
└── docker-compose.yml
```

---

## 17. Roadmap (8 tuần)

| Tuần | Nội dung | Deliverable |
|---|---|---|
| 1 | `rpc-transport`: encode/decode frame (kèm epoch, traceId), Netty echo test | RPC gửi/nhận thành công |
| 2 | `orchestrator` + `worker` cơ bản, single-node, mock LLM | Batch chạy end-to-end single-node |
| 3 | `raft-core`: election + log replication + snapshot/compaction | Demo 3 node bầu lại leader, restart node load từ snapshot |
| 4 | Tích hợp Raft vào orchestrator + epoch/fencing token trên mọi write | Partition leader cũ (không kill) → verify bị reject |
| 5 | WAL/resume + dedup + cache layer + outbox pattern (Postgres↔Redis) | Kill giữa batch, không lặp step, cache/Postgres nhất quán eventual |
| 6 | DAG scheduling (depends_on) + priority scheduling giữa nhiều batch | Batch nhiều bước phụ thuộc chạy đúng thứ tự |
| 7 | `kafka-transport` (thay thế tuỳ chọn cho RPC) + circuit breaker LLM + `analytics` (Spark) | So sánh 2 transport, dashboard đọc metric từ Spark |
| 8 | Chaos test mở rộng (partition/netem), đo metric, README + video demo | Sản phẩm hoàn chỉnh + tài liệu CV/phỏng vấn |

---

## 18. Metric nên đo & đưa vào CV

- Thời gian failover khi kill leader (mục tiêu < 1s) **và** khi partition leader (không kill) — verify 0 write nào từ leader cũ lọt qua sau khi epoch tăng.
- % step chạy lại sau crash (mục tiêu 0%).
- Cache hit rate (30–40% nếu document có mẫu lặp).
- Throughput (document/phút theo N worker), so sánh 2 transport (custom RPC vs Kafka).
- Circuit breaker: số lần OPEN, thời gian trung bình ở OPEN, số request được "cứu" khỏi retry vô ích.
- Consumer lag theo thời gian khi dùng Kafka transport, dưới tải tăng dần.

---

## 19. Câu chuyện kể trong phỏng vấn (gợi ý)

> "Tôi xây một distributed job orchestration engine xử lý hàng loạt tài liệu qua LLM pipeline nhiều bước. Tầng điều phối chạy Raft tự cài đặt, có epoch/fencing token để chống split-brain — không chỉ khi node chết mà cả khi leader cũ bị network-partition vẫn tưởng mình còn là leader. Giao tiếp orchestrator-worker tôi làm 2 phiên bản: một custom wire protocol tự thiết kế để hiểu rõ tầng byte, một bằng Kafka để tận dụng backpressure và scale-out qua consumer lag. Cross-store consistency giữa Postgres và Redis tôi cân nhắc 2PC nhưng chọn transactional outbox pattern vì 2PC blocking không hợp với hệ đã có Raft. Tôi demo bằng cách vừa kill leader vừa partition leader giữa batch 5,000 document nhiều bước phụ thuộc (DAG), hệ thống phục hồi dưới 1 giây, không job nào chạy lại, không leader cũ nào ghi được dữ liệu sau khi epoch đổi."
