# Distributed AI Job Orchestration Engine

Hệ thống điều phối tác vụ AI phân tán (AI Job Orchestration Engine) hỗ trợ lập lịch đồ thị tác vụ (DAG), kiểm soát luồng xử lý theo độ ưu tiên (Deficit Round Robin - DRR), tự phục hồi (Fault Tolerance) sử dụng thuật toán đồng thuận Raft tự triển khai, tích hợp cơ chế ngắt mạch (Circuit Breaker) và giao tiếp bất đồng bộ qua Apache Kafka.

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

Hệ thống gồm 6 modules chính:
1. **`raft-core`**: Core engine của thuật toán đồng thuận Raft (Leader Election, Log Replication, Snapshot & Compaction).
2. **`rpc-transport`**: Giao tiếp nhị phân độ trễ thấp qua Netty TCP (được sử dụng cho nội bộ Raft consensus).
3. **`orchestrator`**: Service trung tâm điều phối các job, quản lý đồ thị DAG, xếp lịch DRR và giao tiếp với Kafka.
4. **`worker`**: Thực thi tác vụ AI giả lập thông qua Mock LLM Client tích hợp Circuit Breaker bảo vệ hệ thống.
5. **`common`**: Các DTO, POJO định dạng event dùng chung.
6. **`frontend`**: Dashboard web (React + Vite + TypeScript) trực quan hóa hệ thống — metrics, trạng thái Raft cluster, quản lý batch/DAG, giám sát worker circuit breaker và chaos demo.

---

## 🛠️ Các Quyết Định Thiết Kế Quan Trọng

### 1. Đồng thuận & Đồng bộ Trạng thái (Raft)
* **Custom Raft Consensus**: Tự xây dựng cấu trúc Raft in-memory thay vì dùng thư viện sẵn để làm chủ hoàn toàn quá trình đồng thuận.
* **Epoch / Fencing Token**: Leader đắc cử sẽ gán `epoch = currentTerm`. Các yêu cầu ghi xuống DB được so khớp `leader_epoch` để từ chối các write path cũ từ leader cũ đã bị cách ly (split-brain).
* **Raft Snapshot & Compaction (Phase 9)**: Giải quyết vấn đề phình to bộ nhớ của log bằng cách "compaction" các log đã commit và gửi qua `InstallSnapshot` RPC khi follower bị lag quá xa. Do state thật của hệ thống nằm ở PostgreSQL, snapshot data được tối ưu hóa chỉ gửi cấu trúc rỗng với `lastIncludedIndex`/`Term` phục vụ fencing & logic log trimming.

### 2. Mô hình Giao tiếp (Kafka làm Broker trung tâm)
* **Orchestrator ↔ Worker**: Ban đầu giao tiếp qua Netty RPC, sau đó nâng cấp hoàn toàn sang **Apache Kafka 3.7.0** ở Phase 8 để tận dụng khả năng lưu trữ đệm, tăng tính decoupling và khả năng mở rộng (Scale Out/In) dễ dàng cho worker.
* **Raft Consensuses**: Vẫn giữ giao tiếp Netty TCP nhị phân trực tiếp độ trễ cực thấp cho các thao tác đồng thuận heartbeat & bầu cử giữa các Orchestrator.

### 3. Giải thuật Điều phối (DAG + DRR Priority)
* **DAG Scheduling**: Cho phép thiết lập phụ thuộc giữa các step (`depends_on`). Chỉ giải phóng (`BLOCKED` → `PENDING`) khi các step tiền đề thành công. Hệ thống tự quét vòng lặp (Cycle Detection) bằng thuật toán DFS Topological Sort trước khi lưu batch.
* **DRR Scheduler (Deficit Round Robin)**: Cơ chế phân phối anti-starvation. Mỗi batch nhận token tích lũy dựa vào priority `(11 - priority)`. StepDispatcher phân phối step dựa trên token dư và khấu trừ dần.

---

## 🚀 Hướng Dẫn Chạy Hệ Thống & Chạy Test

### 1. Yêu cầu hệ thống
* Java 21 & Gradle 9.x
* Docker & Docker Compose

### 2. Chạy Kiểm Thử Tự Động (JUnit)

Chạy test logic Raft Log, Snapshot và Integration Test cluster Netty:
```bash
# Test logic log compaction
./gradlew :raft-core:test --tests "RaftLogTest"

# Test InstallSnapshot RPC handler
./gradlew :raft-core:test --tests "RaftSnapshotTest"

# Test tổng hợp consensus Raft 3 node qua Netty thật trong JVM
./gradlew :orchestrator:test --tests "RaftNettyClusterIntegrationTest"
```

### 3. Build & Khởi Chạy Cụm Distributed Bằng Docker Compose

Build các Fat JAR (tắt plain JAR tránh lỗi đè file rỗng):
```bash
./gradlew build -x test
```

Khởi chạy cluster 3 Orchestrator, 2 Worker, Postgres, Kafka & Redis:
```bash
docker compose up -d --build
```

Kiểm tra log vận hành của cụm Raft:
```bash
docker compose logs -f orchestrator-a orchestrator-b orchestrator-c
```

### 4. Chạy Dashboard Web (Frontend)

Dashboard trực quan hóa hệ thống được xây dựng bằng **React + Vite + TypeScript** (thư mục `frontend/`). Sau khi cluster đã chạy, khởi động frontend:

```bash
cd frontend
npm install
npm run dev
```

Mở trình duyệt tại **http://localhost:5173**. Dashboard gồm các trang:

- **Dashboard** — metrics vận hành (batches, steps completed/reassigned/rerun) + trạng thái Raft cluster (leader, term, epoch) realtime.
- **Batches** — tạo batch đơn giản hoặc batch DAG, xem danh sách batch.
- **Batch Detail** — trực quan hóa DAG bằng React Flow, node đổi màu theo trạng thái step (PENDING/BLOCKED/RUNNING/DONE/FAILED) realtime.
- **Workers** — giám sát circuit breaker (CLOSED/OPEN/HALF_OPEN) và điều khiển fault injection.
- **Chaos Lab** — nút bấm giả lập lỗi (inject fault, kill leader, failover) kèm event log.

> **Lưu ý CORS**: Frontend gọi trực tiếp tới các orchestrator (`8081/8082/8083`) và worker (`8091/8092`). Đã thêm `WebConfig` (CORS) cho phép origin `http://localhost:5173` ở cả `orchestrator` và `worker`.

---

## 💥 Chaos Test & Fault Tolerance (Kịch Bản Tự Động)

Dự án có sẵn script mô phỏng lỗi thực tế [demo_chaos.sh](file:///Users/nganh.cc/Desktop/ai-orchestration-engine/demo_chaos.sh) để kiểm tra khả năng tự phục hồi của cụm. 

Chạy script:
```bash
./demo_chaos.sh
```

**Các bước script tự động thực hiện:**
1. **Submit Job**: Gửi DAG Batch phức tạp lên Leader hiện tại.
2. **Circuit Breaker**: Gửi lỗi liên tiếp vào `worker-1` để kích hoạt trạng thái `OPEN` của Circuit Breaker.
3. **Leader Crash**: Ngắt kết nối mạng của Leader hiện tại ra khỏi container network.
4. **New Election**: Cluster phát hiện Leader offline, tự bầu leader mới và tăng `epoch`.
5. **Epoch Fencing**: Gửi lệnh fake từ leader cũ, DB và các worker tự động từ chối xử lý do stale epoch.
6. **Recovery**: Kết nối lại mạng, phục hồi worker, các step dang dở được re-assign tự động và batch hoàn thành thành công tốt đẹp.
