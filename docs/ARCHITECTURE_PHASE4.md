# Kiến trúc & Luồng hoạt động — Phase 4 (tóm tắt đến code hiện tại)

Tài liệu này mô tả ngắn gọn cách hệ thống chạy tới trạng thái mã hiện tại: các module chính, file quan trọng, luồng RPC giữa `worker` và `orchestrator`, cơ chế fencing/epoch, cập nhật CAS trên DB và pattern outbox.

## Tổng quan modules

- `rpc-transport` — thư viện giao tiếp Netty, mã hóa/giải mã frame, định nghĩa payload cho các message types.
  - `FrameCodec.java`: encode/decode frame theo wire protocol (4-byte length prefix + message fields). Chứa logic encode/decode cho `StepResultPayload`.
  - `StepResultPayload.java`: payload chứa `traceId`, `jobId`, `stepId`, `result` (bytes/string).
  - `RpcHandler.java`: Netty handler / helper để xử lý các frame ở client/worker side; cũng có logic gửi `STEP_RESULT` khi hoàn tất bước.
  - `EchoClient.java` / test helpers: tiện ích kết nối/kiểm tra transport trong dev/tests.

- `orchestrator` — service điều phối: nhận step results, thực hiện CAS update trên `job_step` và insert `outbox` trong một transaction.
  - `JobStepDao.java`: DAO truy vấn/UPDATE `job_step` với điều kiện `leader_epoch <= ?` để thực hiện CAS theo epoch fencing.
  - `StepService.java`: dịch vụ transactional. `markStepDone(stepId, epoch, resultJson)` — gọi DAO UPDATE + insert `outbox` trong cùng transaction.
  - `StepResultRpcHandler.java`: Netty RPC handler phía orchestrator cho message type `STEP_RESULT`. Giải mã frame, đọc `epoch` và `payload`, gọi `StepService.markStepDone`. Nếu update thành công gửi ACK; nếu thất bại do epoch lớn hơn (stale leader), gửi `STALE_LEADER_REJECT`.
  - `WorkerRpcServer.java`: Netty server bootstrap để lắng nghe kết nối từ worker. Có logic bind IPv4 wildcard khi `bindHost` = `0.0.0.0` để tránh chỉ bind IPv6.
  - `OrchestratorRpcBootstrap.java`: Spring `ApplicationRunner` khởi tạo `WorkerRpcServer` khi app start.

- `worker` — agent thực thi step và gửi kết quả về orchestrator.
  - `WorkerRpcClientRunner.java`: Spring runner/Bootstrap cho Netty `Bootstrap` kết nối tới orchestrator; khởi tạo `RpcHandler` client.
  - `RpcHandler` (từ `rpc-transport`): xử lý frame đến (ví dụ `ASSIGN_STEP`), giả lập xử lý/LLM, sau đó gửi `STEP_RESULT` frame chứa `epoch` hiện tại, `requestId`, và `StepResultPayload`.

- `raft-core` (nền tảng Raft, một phần của hệ):
  - Chứa `RaftMessageHandler`, `RaftNode`, `RaftState`, `Snapshot` groundwork. Ở phase 4 mục tiêu là giữ invariant epoch/fencing và snapshot groundwork; tránh thay đổi logic đồng bộ đã test.

- `docker-compose.yml` (dev/demo):
  - Dùng để khởi chạy `postgres`, `orchestrator-a/b/c`, `worker`. Thường map cổng RPC của một orchestrator tới host (ví dụ 9001→9000) và worker trong container dev có `ORCHESTRATOR_HOST=host.docker.internal` để connect tới host-mapped port (dev workaround).

## Wire Protocol (tóm tắt)

- Frame structure (như hiện tại):
  - 4 bytes: length (không tính 4 bytes này)
  - 1 byte: messageType
  - 8 bytes: requestId (long)
  - 8 bytes: epoch (long)
  - N bytes: payload (messageType-specific)

- Message types (ví dụ):
  - `ASSIGN_STEP` — orchestrator→worker: giao step
  - `STEP_RESULT` — worker→orchestrator: kết quả thực thi step
  - `ACK` — xác nhận thành công
  - `STALE_LEADER_REJECT` — orchestrator→worker: reject vì epoch cũ (fencing)

- Ý nghĩa `epoch`: fencing token (leader epoch). Worker gửi lại epoch kèm kết quả để orchestrator kiểm tra leader đã hợp lệ khi cập nhật DB.

## Luồng xử lý end-to-end (worker → orchestrator)

1. Orchestrator (leader) quyết định assign step → gửi `ASSIGN_STEP` frame (có requestId, epoch = current leader epoch) tới worker.
2. Worker nhận `ASSIGN_STEP` trong `RpcHandler`, bắt đầu thực thi (LLM/CTask). Worker giữ `epoch` gốc từ frame và requestId.
3. Worker hoàn tất — đóng gói `StepResultPayload` (traceId, jobId, stepId, resultJson), gửi `STEP_RESULT` frame tới orchestrator với cùng hoặc tách `requestId` mới (hiện mã hóa requestId/điều phối trong `FrameCodec`). Frame chứa `epoch` mà worker biết.
4. Orchestrator nhận `STEP_RESULT` ở `StepResultRpcHandler`:
   - Giải mã payload
   - Gọi `StepService.markStepDone(stepId, epoch, resultJson)`
   - `StepService` thực hiện transaction:
     - `JobStepDao.updateStatusIfEpochAtMost(stepId, epoch, newStatus, resultJson)` — SQL UPDATE ... WHERE `step_id = ? AND leader_epoch <= ?` → trả về rowCount
     - Nếu rowCount == 1: insert row vào `outbox` trong cùng transaction
     - Nếu rowCount == 0: nghĩa là leader epoch trên DB lớn hơn epoch gửi tới (stale) → không update, trả về failure
   - Sau transaction: orchestrator gửi `ACK` nếu success, hoặc `STALE_LEADER_REJECT` nếu stale.
5. Worker nhận ACK hoặc REJECT và tuỳ hành vi có thể retry/tone down.

## Database (Postgres) — các bảng quan trọng

- `job_step` (tối thiểu trường liên quan):
  - `step_id` (PK)
  - `status`
  - `result` (json/text)
  - `leader_epoch` (long/int)
  - `updated_at`

- `outbox` (transactional outbox pattern):
  - `id`, `aggregate_type`, `aggregate_id`, `payload`, `created_at`, `published` flag

Luồng DB: `UPDATE job_step SET status=?, result=?, updated_at=now() WHERE step_id=? AND leader_epoch <= ?` + `INSERT INTO outbox(...)` trong cùng transaction.

## Tests & Helpers

- `rpc-transport` có unit tests (ví dụ `RpcHandlerFenceTest`) để mô phỏng frame send/receive và verify rằng worker gửi `STEP_RESULT` và orchestrator xử lý fencing.
- Thực thi test: `./gradlew :rpc-transport:test` (tương tự cho module `orchestrator` và `raft-core`).

## Dev / Run (tóm tắt nhanh)

- Build local:

```bash
./gradlew build -x test
```

- Start compose demo (dev):

```bash
docker compose up -d --build
```

- Kiểm tra logs:

```bash
docker compose logs -f orchestrator-a worker --tail=200
```

- Kiểm tra DB (ví dụ container name `ai-orchestration-engine-postgres-1`):

```bash
docker exec -it ai-orchestration-engine-postgres-1 psql -U orchestration -d orchestration \
  -c "SELECT * FROM job_step ORDER BY updated_at DESC LIMIT 10;"

docker exec -it ai-orchestration-engine-postgres-1 psql -U orchestration -d orchestration \
  -c "SELECT * FROM outbox ORDER BY created_at DESC LIMIT 10;"
```

## Lưu ý quan trọng & next steps

- `host.docker.internal` được dùng tạm cho demo local để worker trong container connect tới orchestrator host-mapped port. Thay thế bằng cấu hình mạng phù hợp cho môi trường production.
- Outbox publisher chưa được bật trong Phase 4 — cần triển khai poller/publisher để gửi events ra hệ downstream.
- Worker-side retry/dedup logic chưa hoàn thiện: nếu orchestrator trả `STALE_LEADER_REJECT`, cần chính sách retry/backoff hoặc lấy leader mới từ service discovery.
- Bảo đảm tests liên quan đến `raft-core` và `orchestrator` được chạy để tránh phá invariant đã test.

## Liên kết file chính (một số file để xem nhanh)

- `rpc-transport/src/main/java/.../FrameCodec.java`
- `rpc-transport/src/main/java/.../StepResultPayload.java`
- `rpc-transport/src/main/java/.../RpcHandler.java`
- `orchestrator/src/main/java/.../dao/JobStepDao.java`
- `orchestrator/src/main/java/.../service/StepService.java`
- `orchestrator/src/main/java/.../rpc/StepResultRpcHandler.java`
- `orchestrator/src/main/java/.../rpc/WorkerRpcServer.java`
- `orchestrator/src/main/java/.../rpc/OrchestratorRpcBootstrap.java`
- `worker/src/main/java/.../WorkerRpcClientRunner.java`

(Trên đây là bản tóm tắt để bạn có thể dán vào README hoặc PR description — tôi có thể mở rộng thêm sơ đồ sequence, mermaid chart, hoặc chi tiết SQL nếu cần.)
