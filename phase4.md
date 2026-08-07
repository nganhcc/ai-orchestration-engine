## Plan: Hoàn thiện Phase 4

TL;DR - Mục tiêu: Hoàn thiện fencing (epoch) end-to-end, đảm bảo DB CAS trên `job_step` với `leader_epoch`, chèn transactional outbox, worker → orchestrator STEP_RESULT flow, tests tích hợp và demo docker. Giải pháp: hoàn thiện codec + handler RPC, kiểm tra CAS trong transaction, viết migration nếu cần, thêm outbox poller tạm thời, viết e2e tests (docker-compose) và metrics/logging cơ bản.

**Steps**
1. Mã hóa / RFC giao thức (small):
- **Mô tả**: xác nhận wire frame định nghĩa (length + messageType(1) + requestId(8) + epoch(8) + payload).
- **Task**: kiểm tra `rpc-transport` codec cho `STEP_RESULT` và trường `epoch` luôn đọc/ghi.
- **Depends on**: hiện có codec cơ bản.

2. Orchestrator: StepResult handler → transactional CAS + outbox
- **Mô tả**: `StepResultRpcHandler` phải decode payload, gọi `StepService.markStepDone(stepId, epoch, resultJson)`.
- **Task**: đảm bảo `StepService.markStepDone` thực hiện:
  - gọi DAO `updateStatusIfEpochAtMost(stepId, epoch, status, resultJson)`
  - nếu DAO trả thành công (rowsUpdated == 1) thì chèn vào `outbox` trong cùng transaction
  - trả ACK cho worker; nếu rowsUpdated == 0 trả `STALE_LEADER_REJECT`.
- **Files**:
  - [orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/service/StepService.java](orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/service/StepService.java)
  - [orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/dao/JobStepDao.java](orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/dao/JobStepDao.java)
  - [orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/rpc/StepResultRpcHandler.java](orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/rpc/StepResultRpcHandler.java)
- **Verification**: unit tests mocking DAO and integration test hitting real DB.

3. DB: migrations & schema checks
- **Mô tả**: đảm bảo `job_step` có cột `leader_epoch` (bigint) và `outbox` table tồn tại.
- **Task**: bổ sung Flyway migration (nếu chưa có) tạo/alter cột và index; thêm retention policy comment.
- **Files**: `orchestrator/src/main/resources/db/migration/V__*.sql` (create or modify)
- **Verification**: run Flyway locally / run compose and query DB.

4. Worker: send STEP_RESULT + handle ACK/STALE
- **Mô tả**: Worker phải gửi `STEP_RESULT` frame kèm `epoch` mà nó dùng để thực thi step; xử lý `ACK` và `STALE_LEADER_REJECT`:
  - khi nhận `ACK` → mark local done
  - khi nhận `STALE_LEADER_REJECT` → fetch current leader epoch/metadata hoặc abort và surface error
- **Files**:
  - [worker/src/main/java/com/nganhcc/orchestration/worker/WorkerRpcClientRunner.java](worker/src/main/java/com/nganhcc/orchestration/worker/WorkerRpcClientRunner.java)
  - [rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/RpcHandler.java](rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/RpcHandler.java)
- **Verification**: unit tests for handler logic; integration run with orchestrator to confirm ACK/STALE flows.

5. Outbox publisher (minimum viable)
- **Mô tả**: để Phase 4 có end-to-end observable, implement simple poller that reads `outbox` and logs/prints payload (real publisher in later phases).
- **Task**: add a Spring scheduled component `OutboxPublisher` that queries unprocessed rows and marks `sent_at` when done.
- **Files**:
  - [orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/outbox/OutboxPublisher.java](orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/outbox/OutboxPublisher.java)
  - DAO/migration for `outbox`.
- **Verification**: run compose, trigger step, observe `outbox` inserted and logged by poller.

6. Tests: Unit + Integration + e2e
- **Unit tests**:
  - `StepService` behavior for success/stale paths
  - `JobStepDao.updateStatusIfEpochAtMost` (use embedded Postgres or mocked JDBC)
  - `rpc-transport` codec/frame tests
- **Integration tests (fast)**:
  - JUnit test that starts Spring context + Testcontainers Postgres, sends a `StepResultRpcHandler` call programmatically and asserts DB rows.
- **E2E (docker-compose)**:
  - Compose up orchestrator + worker + postgres; run a job assignment, worker sends `STEP_RESULT`, then assert `job_step` updated and `outbox` has entry.
- **Files**:
  - tests under `orchestrator/src/test/java/...` `rpc-transport/src/test/java/...` `worker/src/test/java/...`
- **Verification**: add CI job to run these.

7. Observability & metrics (basic)
- **Mô tả**: add logs and counters for `stale_reject_count`, `step_results_received`, `outbox_queue_size`.
- **Task**: use Micrometer to register counters in `StepResultRpcHandler` and `OutboxPublisher`.
- **Files**: relevant orchestrator classes.

8. Docs & runbook
- **Mô tả**: update README và `PROGRESS.md` với run steps, how to validate DB rows and logs, and known dev-workarounds (host.docker.internal).
- **Files**: `README.md`, `PROGRESS.md`, `AGENT.md` if necessary.

9. Cleanup & hardening
- **Mô tả**: remove test hooks guarded by `@TestOnly` or env flags, avoid shipping debug env changes.
- **Task**: review code for `setHighestEpochForTests` hoặc hardcoded configs.

**Relevant files**
- [rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/FrameCodec.java](rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/FrameCodec.java) — encode/decode frames
- [rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/StepResultPayload.java](rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/StepResultPayload.java) — payload model
- [rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/RpcHandler.java](rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/RpcHandler.java) — worker client handler
- [orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/rpc/StepResultRpcHandler.java](orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/rpc/StepResultRpcHandler.java) — orchestrator RPC handler
- [orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/service/StepService.java](orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/service/StepService.java)
- [orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/dao/JobStepDao.java](orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/dao/JobStepDao.java)
- [orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/outbox/OutboxPublisher.java](orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/outbox/OutboxPublisher.java) (new)
- `docker-compose.yml` — dev demo wiring

**Verification**
1. Unit tests for service/dao/codec pass locally (`./gradlew :rpc-transport:test :orchestrator:test :worker:test`).
2. Integration test with Testcontainers Postgres: `StepService.markStepDone` success/stale cases.
3. E2E: `docker compose up -d --build`, run job assignment, assert via psql:
   - `SELECT * FROM job_step WHERE step_id = '<stepId>';` shows updated status and `leader_epoch` unchanged or set as expected.
   - `SELECT * FROM outbox ORDER BY created_at DESC LIMIT 5;` shows inserted message.
4. Logs: orchestrator logs `orchestrator.rpc.started` and `StepResult processed` lines; worker logs `STEP_RESULT sent` and `ACK/STALE` handling.

**Decisions**
- Use `leader_epoch <= :epoch` to allow leader with equal epoch to write (fencing semantics). If we want strict equality, change to `=`.
- Phase 4 will include a minimal OutboxPublisher that logs instead of delivering to external system (full publisher in Phase 5).
- Keep `host.docker.internal` as dev-only documented workaround; plan to replace in Phase 5.

**Further Considerations**
1. Do you prefer `leader_epoch <= epoch` (allows same or older leader) or strict `=`? Recommendation: `<=` with epoch monotonic increasing on leader election.
2. For E2E CI, use Testcontainers rather than full docker-compose to keep CI faster.
3. If you want, I can implement: (a) Flyway migration, (b) `OutboxPublisher` minimal, (c) unit tests for `StepService` — pick which to start.
