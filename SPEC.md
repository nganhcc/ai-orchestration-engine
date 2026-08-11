# Distributed AI Job Orchestration Engine — Đặc tả kỹ thuật cốt lõi

## 1. Tech Stack & Cấu trúc dự án
- **Core**: Java 21, Spring Boot 3.x, Gradle Kotlin DSL (`.kts`).
- **Network**: Netty (TCP) cho custom RPC protocol.
- **Storage**: PostgreSQL 16 (metadata, WAL, Outbox) + Redis 7 (cache-aside).
- **Cơ chế đồng thuận**: Raft consensus tự triển khai (`raft-core`).
- **Cấu trúc multi-module**:
  - `raft-core`: Thuật toán bầu cử, nhân bản log, snapshot và epoch/fencing.
  - `rpc-transport`: Netty TCP, custom wire protocol nhị phân.
  - `orchestrator`: REST API, Job Scheduler (DAG + DRR Priority), DB Repository.
  - `worker`: LLM client, circuit breaker, connection handler.

---

## 2. Định dạng Wire Protocol (Netty Frame)
Mọi gói tin truyền qua mạng (cả nội bộ Raft lẫn Orchestrator ↔ Worker) đều kế thừa định dạng `FrameMessage` (Length-Prefixed, Big-Endian):

```
+---------------------------+----------------+----------------+-------------------+--------+
| totalLength (int, 4 B)    | messageType(1B)| requestId(8B)  | epoch (8B)        | payload|
+---------------------------+----------------+----------------+-------------------+--------+
```

- **`totalLength`**: Độ dài của phần phía sau nó, bằng `1 + 8 + 8 + N` bytes (không tính 4 bytes của chính field `totalLength`).
- **`messageType`**:
  - Raft internal: `0x01` (RequestVote), `0x02` (AppendEntries).
  - Worker RPC: `0x05` (AssignStep), `0x06` (StepResult).
  - Khác: `0x03` (Heartbeat), `0x04` (Ack).
- **`epoch`**: Fencing token. Leader set `epoch = currentTerm` khi đắc cử. Node hạ cấp (step-down) set `epoch = 0`.
- **`requestId`**: ID tăng dần tuần tự dùng để khớp Request/Response.

**Giới hạn Netty**: `LengthFieldBasedFrameDecoder` cấu hình `maxFrameLength = 1MB`.

---

## 3. Database Schema (PostgreSQL)

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

CREATE TABLE outbox (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,           -- CACHE_INVALIDATE, STEP_DONE_NOTIFY
    payload JSONB NOT NULL,
    published BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
```

---

## 4. Raft State & Fencing Token
- **State variables**:
  - `currentTerm`: Term hiện tại của node.
  - `epoch`: Bằng `currentTerm` khi chuyển sang trạng thái `LEADER`. Bằng `0` khi lùi về `FOLLOWER`.
- **Fencing Rule**:
  - Khi gửi bất kỳ yêu cầu biến đổi dữ liệu (ví dụ: Assign Step tới Worker), Leader bắt buộc truyền kèm `epoch` hiện tại của mình trong header.
  - Phía nhận (Worker / Database write path) sẽ thực hiện so khớp: nếu `epoch_gui < epoch_hien_tai` → Từ chối xử lý (`STALE_LEADER`).
  - Lệnh cập nhật DB luôn kiểm tra điều kiện `WHERE leader_epoch <= :epoch` (Compare-and-Swap ở tầng SQL).

---

## 5. DAG & DRR Priority Scheduling
- **DAG Routing**:
  - Step có danh sách `depends_on` không rỗng được lưu ở trạng thái `BLOCKED`.
  - Khi một step hoàn thành (`DONE`), tiến hành cập nhật phụ thuộc trong cùng transaction: load các step phụ thuộc và unblock chúng (`BLOCKED` → `PENDING`) nếu toàn bộ dependency đã đạt trạng thái `DONE`.
  - Kiểm tra chu kỳ (cycle detection) bằng giải thuật DFS topological sort trước khi lưu trữ batch.
- **Deficit Round Robin (DRR)**:
  - Phân phối công bằng các step cho các Worker rảnh dựa theo `batch_job.priority`.
  - Mỗi active batch nhận được số lượng token tích lũy `quantum = (11 - priority)` trên mỗi vòng điều phối.
  - StepDispatcher chỉ gửi các step thuộc về batch có deficit counter > 0, sau đó khấu trừ token dựa trên số step đã gửi.

---

## 6. Circuit Breaker cho LLM Client
- Trạng thái: `CLOSED` (bình thường), `OPEN` (chặn gọi, trả lỗi ngay lập tức), `HALF_OPEN` (cho phép thử lại một số lượng request giới hạn).
- Chuyển trạng thái: Quá ngưỡng lỗi liên tiếp (N) → `OPEN`. Sau thời gian cooldown → `HALF_OPEN`. Thử lại thành công → `CLOSED`, thất bại → `OPEN` tiếp.
- Step bị lỗi do provider degrades (`OPEN`) được đánh dấu `FAILED` tạm thời và đưa vào hàng đợi retry đặc biệt, không tính trực tiếp vào quota retry tối đa của step.
