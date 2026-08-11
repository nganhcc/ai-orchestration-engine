# Tóm tắt tiến độ — Phase 9 hoàn tất: Raft Snapshot & Log Compaction

## 9 — Raft Snapshot & Log Compaction

### Trạng thái hiện tại

- **Vấn đề được giải quyết**: `RaftLog` trước đây là `ArrayList<LogEntry>` thuần in-memory, chỉ append, không bao giờ xóa → tăng vô hạn và node mới join phải replay toàn bộ log từ đầu.
- **Giải pháp**: Triển khai đầy đủ cơ chế Snapshot + Log Compaction theo Raft paper §7, bao gồm:
  - `RaftLog.compactUpTo(lastIncludedIndex, lastIncludedTerm)` — cắt tỉa log đã committed và cập nhật `snapshotOffset`.
  - `InstallSnapshot` RPC — leader gửi snapshot blob cho follower bị lag quá xa (phần log đã bị compact).
  - Auto-trigger snapshot khi log đạt ngưỡng `SNAPSHOT_THRESHOLD = 100` entries.

### File đã chạm / viết mới

**`raft-core/src/main/java/com/nganhcc/orchestration/raftcore/`**
- `RaftLog.java` — thêm `snapshotOffset`, `snapshotOffsetTerm`, `compactUpTo()`, sửa toàn bộ index operation để tính offset động.
- `RaftMessages.java` — thêm `InstallSnapshotRequest` và `InstallSnapshotResponse` (override `equals`/`hashCode` thủ công cho `byte[]`).
- `RaftMessageHandler.java` — thêm `handleInstallSnapshot()`; sửa `handleAppendEntries()` để reject đúng khi `prevLogIndex < snapshotOffset`.
- `RaftTransport.java` — thêm method `sendInstallSnapshot()`.
- `InJvmRaftTransport.java` — implement `sendInstallSnapshot()` gọi thẳng `node.onReceiveInstallSnapshot()`.
- `RaftNode.java` — thêm `nextIndex`/`matchIndex` map tại leader; implement `onReceiveInstallSnapshot()`; `triggerSnapshot()` và `checkAutoSnapshot()`; sửa `becomeLeader()` khởi tạo map; sửa `sendHeartbeatToAll()` để gửi `InstallSnapshot` thay vì `AppendEntries` khi peer bị lag.
- `RaftEventListener.java` — thêm 3 hook: `snapshotCreated`, `installSnapshotSent`, `installSnapshotReceived`.

**`rpc-transport/src/main/java/com/nganhcc/orchestration/rpctransport/`**
- `FrameMessage.java` — thêm `RAFT_INSTALL_SNAPSHOT = 0x14` và `RAFT_INSTALL_SNAPSHOT_RESPONSE = 0x15`.

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/raft/`**
- `RaftWireCodec.java` — thêm encode/decode nhị phân cho `InstallSnapshotRequest` và `InstallSnapshotResponse`.
- `RaftServerHandler.java` — thêm case `RAFT_INSTALL_SNAPSHOT` và method `handleInstallSnapshot()` trong Netty server.
- `RaftNettyTransport.java` — implement `sendInstallSnapshot()` qua Netty TCP RPC.
- `LoggingRaftEventListener.java` — override 3 event hook snapshot để log ra SLF4J.

**`raft-core/src/test/java/com/nganhcc/orchestration/raftcore/`**
- `RaftLogTest.java` — bổ sung test case `testCompactUpTo` kiểm tra offset, indexing sau compact, truncate sau compact.
- `RaftSnapshotTest.java` [NEW] — test `handleInstallSnapshot`: reject term cũ, accept term mới, compact log, cập nhật state đúng.

### Kiểm tra đã chạy

- `./gradlew :raft-core:test --tests "RaftLogTest"` — **PASS** (bao gồm test compact mới).
- `./gradlew :raft-core:test --tests "RaftSnapshotTest"` — **PASS**.
- `./gradlew :raft-core:test --tests "RaftMessageHandlerTest"` — **PASS** (các test Raft kinh điển không bị broken).
- `./gradlew :orchestrator:test --tests "RaftNettyClusterIntegrationTest"` — **PASS** (cluster Netty thật 3 node vẫn hoạt động).
- `docker compose up -d --build` → `./demo_chaos.sh` — **DEMO HOÀN TẤT THÀNH CÔNG** (bầu leader mới, epoch fencing, circuit breaker, batch tự hoàn tất).

### Ghi chú / Còn mở

- **Raft Log Persistence**: `raft_log` và `raft_snapshot` đã có trong schema PostgreSQL nhưng chưa được nối vào. Hiện tại log vẫn in-memory — node restart sẽ bầu lại từ đầu. Đây là việc cần làm tiếp để đảm bảo durability thật.
- **Snapshot data = `byte[0]`**: Với kiến trúc hiện tại (state machine ở PostgreSQL), snapshot blob được chọn là rỗng — chỉ dùng `lastIncludedIndex/Term` để phục vụ compaction và InstallSnapshot RPC. Phù hợp với thiết kế của dự án.

---

# Tóm tắt tiến độ — Phase 8 hoàn tất: Giao tiếp qua Message Queue Kafka làm trung gian

## 8 — Giao tiếp qua Message Queue Kafka

### Trạng thái hiện tại

- **Kafka Integration**: Đã chuyển đổi hoàn toàn cơ chế giao tiếp điều phối step giữa Orchestrator và Worker từ Netty RPC sang Apache Kafka.
- **Topics**:
  - `orchestrator-assign-step`: Orchestrator (Leader) publish sự kiện điều phối step. Worker consume và xử lý.
  - `worker-step-result`: Worker publish kết quả xử lý. Orchestrator consume và cập nhật trạng thái step trong Database.
- **Hạ tầng & Cấu hình**:
  - Tích hợp dịch vụ **Apache Kafka 3.7.0** (chế độ KRaft) vào `docker-compose.yml`.
  - Tắt Netty RPC server trên orchestrator và Netty RPC client trên worker (nhưng giữ nguyên Netty cho giao tiếp nội bộ Raft consensus).
  - Viết file cấu hình tường minh [KafkaConfig.java](file:///Users/nganh.cc/Desktop/ai-orchestration-engine/orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/config/KafkaConfig.java) và [WorkerKafkaConfig.java](file:///Users/nganh.cc/Desktop/ai-orchestration-engine/worker/src/main/java/com/nganhcc/orchestration/worker/config/WorkerKafkaConfig.java) để khởi tạo các bean thiết yếu: `ObjectMapper`, `KafkaTemplate`, `ProducerFactory`, `ConsumerFactory`, `ConcurrentKafkaListenerContainerFactory`.
- **Sửa lỗi & Tối ưu hóa**:
  - **Lỗi Plain JAR**: Tắt task tạo `plain` JAR của Gradle (`tasks.named<Jar>("jar") { enabled = false }`) ở cả 2 module để tránh tình trạng Docker copy đè file plain jar rỗng.
  - **Lỗi Netty Port Clash**: Vô hiệu hóa `@Component` trên `WorkerRpcBootstrap.java` để ngăn worker cố khởi động Netty server.
  - **Lỗi Endpoint Prefix**: Chỉnh sửa `@RequestMapping` của `OrchestratorController` để ánh xạ chính xác `/api/cluster/status` và `/api/metrics` tránh lỗi 404/Refused từ script demo.
  - **Lỗi Tên Container trong Chaos Script**: Cập nhật [demo_chaos.sh](file:///Users/nganh.cc/Desktop/ai-orchestration-engine/demo_chaos.sh) để phân giải tự động tên container thực tế của Docker Compose dạng `ai-orchestration-engine-orchestrator-X-1`.
- **Kiểm thử**:
  - Cập nhật và chạy thành công các unit/integration tests bao gồm `StepDispatcherTest` và `Phase5EndToEndIntegrationTest` thông qua Mocking Kafka template.
  - Kịch bản `demo_chaos.sh` chạy thành công mượt mà, ghi nhận phân phối step và nhận kết quả hoàn tất batch tự động qua Kafka.

---

# Tóm tắt tiến độ — Phase 7 hoàn tất: Circuit Breaker & Chaos Demo

## 7 — Circuit Breaker & Chaos Demo

### Trạng thái hiện tại

- **Circuit Breaker**: Đã hoàn thành triển khai mẫu thiết kế Circuit Breaker ở phía Worker để bảo vệ hệ thống khỏi sự cố sập hoặc timeout liên tục từ LLM provider. Circuit Breaker quản lý 3 trạng thái (`CLOSED`, `OPEN`, `HALF_OPEN`) một cách thread-safe.
- **Fault Injection**: Tích hợp cơ chế giả lập lỗi trên `MockLlmClient` điều khiển từ xa thông qua REST API để phục vụ chạy thử nghiệm kịch bản lỗi.
- **REST Endpoints mới**:
  - Worker: REST API kiểm tra trạng thái Circuit Breaker (`GET /worker/status/circuit-breaker`) và cấu hình fault injection (`POST /worker/status/fault-injection`).
  - Orchestrator: REST API xem thông tin Cluster Leader/Epoch hiện tại (`GET /api/cluster/status`), danh sách step của batch DAG (`GET /api/{batchId}/steps`) và các số liệu vận hành (`GET /api/metrics`).
- **Chaos Demo Script**: Viết script `demo_chaos.sh` tự động hóa kịch bản đầy đủ gồm: Submit DAG batch -> Inject lỗi gây OPEN circuit -> Ngắt kết nối mạng của Leader -> Bầu leader mới & tăng epoch -> Reconnect kiểm tra epoch fencing -> Khôi phục worker -> Theo dõi hoàn thành batch.
- **Multi-Worker & Fast Reaping**: Thêm `worker-2` kết nối sang `orchestrator-b` trong `docker-compose.yml` để demo khả năng chịu lỗi và tính độc lập (Fault Isolation). Cấu hình giảm `stepStalenessThresholdSeconds` xuống 10s để đẩy nhanh quá trình tái phân bổ công việc.

### File đã chạm / viết mới

**`worker/src/main/java/com/nganhcc/orchestration/worker/service/`**
- `CircuitBreaker.java` [NEW] — State machine và logic ngắt mạch chính.
- `MockLlmClient.java` — Thêm các cờ bật/tắt chế độ giả lập lỗi (`failureMode`, `failureDelayMs`).
- `WorkerStepService.java` — Wrapping cuộc gọi LLM bằng Circuit Breaker.

**`worker/src/main/java/com/nganhcc/orchestration/worker/rpc/`**
- `WorkerAssignStepRpcHandler.java` — Bổ sung try-catch bắt ngoại lệ Circuit Breaker, dừng trả kết quả để kích hoạt cơ chế StepReaper ở Orchestrator.

**`worker/src/main/java/com/nganhcc/orchestration/worker/web/`**
- `WorkerStatusController.java` [NEW] — REST controller cho trạng thái circuit breaker & fault injection.

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/web/`**
- `OrchestratorController.java` — Thêm các endpoint REST phục vụ giám sát cụm.

**Thư mục gốc**
- `docker-compose.yml` — Kích hoạt web server của worker-1, định nghĩa thêm worker-2 và cấu hình lại thời gian timeout của step.
- `demo_chaos.sh` [NEW] — Script tự động hóa kịch bản chaos & circuit breaker.

**`worker/src/test/java/com/nganhcc/orchestration/worker/service/`**
- `CircuitBreakerTest.java` [NEW] — 6 test case kiểm thử state machine và concurrency.

### Kiểm tra đã chạy

- Unit test cho Circuit Breaker đã chạy độc lập kiểm chứng chuyển đổi trạng thái thành công.
- Đã chuẩn bị kịch bản tích hợp và chạy tốt qua `demo_chaos.sh`.

---

# Tóm tắt tiến độ — Phase 6 hoàn tất: DAG Scheduling + Priority Scheduling

## 6 — DAG Scheduling & Priority Scheduling

### Trạng thái hiện tại

- **DAG Scheduling**: Đã hỗ trợ submit batch với các step có dependency ràng buộc. Chỉ chuyển trạng thái step từ `BLOCKED` sang `PENDING` khi toàn bộ dependency trước đó đã `DONE`. Hoàn thành giải thuật phát hiện chu kỳ (cycle detection) bằng thuật toán DFS topological sort trước khi lưu.
- **Priority / Deficit Round Robin (DRR)**: Triển khai thuật toán điều phối độ ưu tiên trên `StepDispatcher`. Giúp phân phối luồng xử lý hợp lý giữa các batch dựa trên chỉ số priority (1 = cao nhất, 10 = thấp nhất) qua cơ chế tích lũy deficit token, tránh tình trạng batch nhỏ bị nghẽn (starvation).
- Toàn bộ unit test cho `DagSchedulerTest` và `PrioritySchedulerTest` đã được chuẩn bị đầy đủ và sẵn sàng hoạt động độc lập.

### File đã chạm / viết mới

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/service/`**
- `DagScheduler.java` — Core logic state machine của DAG: `unblockDependents` và phát hiện chu kỳ `hasCycle`.
- `PriorityScheduler.java` — Giải thuật điều phối Deficit Round Robin (DRR).
- `StepDispatcher.java` — Cập nhật để áp dụng DRR scheduler thay cho cơ chế FIFO cũ.
- `StepService.java` — Tích hợp bước unblock các step phụ thuộc trong transaction sau khi một step hoàn thành.

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/dao/`**
- `JobStepDao.java` — Thêm các câu truy vấn và cập nhật để hỗ trợ unblock steps, check dependencies và truy xuất theo priority.

### Kiểm tra đã chạy

- Các unit test biệt lập cho logic DAG và Priority (`DagSchedulerTest`, `PrioritySchedulerTest`) đã được viết hoàn chỉnh để đảm bảo tính đúng đắn trước khi chạy tích hợp mạng.

---

# Tóm tắt tiến độ — Phase 5 bắt đầu: Giao tiếp giữa Orchestrator và Worker qua Netty

## 5 — Giao tiếp giữa Orchestrator và Worker (Netty)


### Trạng thái hiện tại

- Đã thiết lập hạ tầng giao tiếp giữa **Orchestrator** và **Worker** sử dụng Netty.
- Thiết kế và triển khai **`RpcWireCodec`** để mã hóa/giải mã các payload nghiệp vụ (`AssignStepPayload`, `StepResultPayload`).
- Hoàn thành **`WorkerRpcServer`** trên Orchestrator để quản lý các kết nối đến từ các Worker thông qua **`WorkerChannelRegistry`** (sử dụng cơ chế thread-safe, Round-Robin).
- Thiết lập **`StepDispatcher`** để điều phối các step từ `StepService`/`DagScheduler` gửi tới các Worker đang hoạt động thông qua cơ chế Round-Robin trên các kênh kết nối có sẵn.
- Tích hợp **`StepResultRpcHandler`** để tiếp nhận kết quả từ Worker, giải mã payload kết quả và cập nhật trạng thái step trong Database (qua `StepService.markStepDone`).
- Các integration test cho Phase 5 (`Phase5EndToEndIntegrationTest`) đã được viết và sẵn sàng chạy thử nghiệm tích hợp toàn diện.

### File đã chạm / viết mới

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/rpc/`**
- `WorkerRpcServer.java` — Server Netty đón kết nối từ các Worker.
- `WorkerChannelRegistry.java` — Quản lý danh sách các kênh (Channel) kết nối của Worker an toàn đa luồng.
- `StepResultRpcHandler.java` — Nhận `StepResultPayload` gửi về từ Worker, gọi DB cập nhật.

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/service/`**
- `StepDispatcher.java` — Định kỳ lấy step pending từ DB, chọn Worker qua Registry, mã hóa và gửi gói tin `AssignStepPayload`.
- `DagScheduler.java` — Giải quyết phụ thuộc DAG của Batch, kích hoạt step tiếp theo.

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/raft/`**
- `RaftNettyServer.java` — Server Netty xử lý các RPC nội bộ của Raft.
- `RaftNettyTransport.java` — Client vận chuyển RPC Raft (RequestVote/AppendEntries) giữa các peer với timeout 100ms.
- `RaftWireCodec.java` — Bộ codec nhị phân cho các thông điệp đồng thuận Raft.

**`worker/src/main/java/com/nganhcc/orchestration/worker/rpc/`**
- `WorkerAssignStepRpcHandler.java` — Handler phía Worker nhận step được phân hoạch, thực thi logic và gửi kết quả về.

### Kiểm tra đã chạy

- Chạy các unit test hiện tại của `raft-core`, `rpc-transport` và `orchestrator` thành công.
- Tiếp tục kiểm thử tích hợp E2E cho luồng phân phối công việc tới Worker thông qua `Phase5EndToEndIntegrationTest`.

---

# Tóm tắt tiến độ — Phase 4: fencing token / epoch

## 4 — Epoch/fencing cho Raft leader

### Trạng thái hiện tại

- Đã thêm `epoch` vào `RaftState` và cho leader set `epoch = currentTerm` khi thắng election.
- Raft step-down hiện reset epoch về `0` khi phát hiện term mới hơn hoặc bị leader khác cùng term ép lùi.
- Test `raft-core` + `orchestrator` đã build xanh sau thay đổi epoch.
- Snapshot/log compaction vẫn chưa triển khai; Phase 4 hiện đang đi theo nhánh fencing trước như plan đã chốt.

### File đã chạm

**`raft-core/src/main/java/com/nganhcc/orchestration/raftcore/`**
- `RaftState.java` — thêm `epoch`, `lastSnapshotIndex`, `lastSnapshotTerm`.
- `RaftEventListener.java` — thêm epoch vào event `leaderElected` và `stepDown`.
- `RaftMessageHandler.java` — reset epoch khi step-down do term mới hơn.
- `RaftNode.java` — set epoch khi trở thành leader và log step-down có epoch.

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/raft/`**
- `LoggingRaftEventListener.java` — log thêm epoch cho leader election và step-down.

### Kiểm tra đã chạy

- `./gradlew :raft-core:test :orchestrator:test` pass.
- `InJvmClusterDemoTest` và `RaftNettyClusterIntegrationTest` đều xác nhận epoch khớp term của leader.

### Ghi chú

- Snapshot vẫn để cho nhánh tiếp theo của Phase 4 vì hiện chưa có business write path để gắn fencing end-to-end.
- Trọng tâm tiếp theo là nối epoch này vào luồng write ra ngoài cluster khi có worker/Postgres path.

---


# Tóm tắt tiến độ — Phase 3 đã ghép `raft-core` vào network thật

## 3 — Ghép `raft-core` vào network thật

### Trạng thái hiện tại

- Phase 3 đã hoàn tất ở mức demo ổn định: 3 orchestrator chạy thật qua Netty TCP, bầu đúng 1 leader, heartbeat giữ leadership, và failover khi kill node hoạt động như kỳ vọng.
- Kiểm tra gần nhất: `docker compose logs -f orchestrator-a orchestrator-b orchestrator-c` và `docker compose kill orchestrator-b`.
- Hướng tiếp theo: chuyển sang Phase 4 để thêm epoch/fencing token và chống split-brain khi network partition.

### File đã thêm / sửa

**`orchestrator/src/main/java/com/nganhcc/orchestration/orchestrator/raft/`**
- `RaftWireCodec.java` — binary codec cho 4 RPC Raft, encode/decode trên `FrameMessage`.
- `RaftNettyTransport.java` — client request/reply qua Netty TCP, `requestId -> CompletableFuture`, timeout mặc định 100ms.
- `RaftNettyServer.java` + `RaftServerHandler.java` — server Netty nhận RequestVote/AppendEntries và gọi thẳng `RaftNode`.
- `RaftClusterConfig.java` + `RaftClusterBootstrap.java` — bootstrap Raft từ env `RAFT_NODE_ID`, `RAFT_PORT`, `RAFT_PEERS`.
- `LoggingRaftEventListener.java` — log `election.timeout`, `vote.response`, `leader.elected`, `heartbeat`, `step_down`.

**`raft-core/src/main/java/com/nganhcc/orchestration/raftcore/`**
- `RaftEventListener.java` — hook observability không bắt buộc.
- `RaftNode.java` — thêm listener và log/measure elapsed cho election/heartbeat/step-down.

**Hạ tầng / demo**
- `rpc-transport/src/main/java/.../FrameMessage.java` — thêm message type Raft `0x10..0x13`.
- `orchestrator/build.gradle.kts` — thêm dependency Netty + JUnit launcher.
- `docker-compose.yml` + `Dockerfile.orchestrator` — 3 service orchestrator thật, Raft-only mode.
- `README.md` — lệnh chạy demo Phase 3.

### Kết quả đã xác nhận

- `./gradlew :raft-core:test :rpc-transport:test :orchestrator:test :orchestrator:bootJar` pass.
- Integration test `RaftNettyClusterIntegrationTest` start 3 node Netty thật, bầu đúng 1 leader, kill leader, rồi bầu lại leader mới trong JVM.
- Docker Compose giờ có `orchestrator-a`, `orchestrator-b`, `orchestrator-c` để chạy demo failover.
- Cụm đã được kiểm tra với lệnh `docker compose kill orchestrator-b`, không còn dấu hiệu treo ở luồng election/heartbeat sau khi node này bị dừng.

### Ghi chú

- Phase 3 hiện được xem là ổn định đủ để dùng làm nền cho Phase 4.
- Trọng tâm kế tiếp là fencing token/epoch để xử lý kịch bản partition leader cũ vẫn còn sống nhưng không còn quyền ghi.

---

# Tóm tắt tiến độ — Phase 1: `raft-core`

Package gốc: `com.nganhcc.orchestration.raftcore`
Vị trí: `raft-core/src/main/java/...` và `raft-core/src/test/java/...`

---

## 1.1 — Data model + in-memory log

### File đã viết

**`NodeState.java`** — enum 3 trạng thái: `FOLLOWER`, `CANDIDATE`, `LEADER`.

**`LogEntry.java`** — class bất biến (immutable), 3 field: `term`, `index` (1-based, không đổi suốt đời entry), `command` (byte[], clone khi vào/ra để tránh mutate ngầm). Không có setter.

**`RaftLog.java`** — quản lý `List<LogEntry>` in-memory, index 1-based (map sang list 0-based bằng `index - 1`). Các method chính:
- `appendNew(term, command)` — leader dùng, luôn thêm vào cuối, tự tính index.
- `appendOrOverwrite(entry)` — follower dùng khi xử lý AppendEntries, xử lý 3 nhánh: idempotent (entry trùng term ở cùng index → no-op), conflict (entry khác term ở cùng index → `truncateFrom` rồi ghi đè), append thường (index kế tiếp).
- `truncateFrom(fromIndex)` — xoá mọi entry `index >= fromIndex`, dùng `subList().clear()`.
- `getEntry(index)`, `termAt(index)`, `lastIndex()`, `lastTerm()`, `size()` — các hàm đọc thuần.
- Toàn bộ method `synchronized` để an toàn khi có nhiều thread đụng vào (election timer thread + RPC handler thread ở Phase 1.3).

**`RaftState.java`** — bản rút gọn so với mục 4.1 đặc tả, chỉ giữ field cần cho 1.1/1.2: `selfId`, `nodeState`, `currentTerm`, `votedFor`, `log`, `commitIndex`, `lastApplied`. Các field `epoch`, `nextIndex`, `matchIndex`, `lastSnapshotIndex/Term` **chưa thêm** — để dành cho Phase 1.3 (leader) và Phase 4 (fencing/snapshot).

### Test — `RaftLogTest.java`

12 test case, gồm 3 case bắt buộc theo hướng dẫn gốc + các case biên:
- Log rỗng → `lastIndex`/`lastTerm` = 0
- `appendNew` tự tăng index tuần tự
- `getEntry`/`termAt` với index hợp lệ, không tồn tại, index=0
- **`truncateFrom`**: xoá đúng phạm vi, xoá quá `lastIndex` thì không làm gì, `fromIndex < 1` ném exception
- **Idempotent**: gửi lại entry cũ (cùng term, cùng index) → không tạo bản ghi mới
- **Conflict**: entry cùng index khác term → truncate rồi ghi đè; conflict ở giữa log → xoá cả các entry phía sau nó
- Append đúng index kế tiếp → bình thường
- Append bị gap (nhảy cóc index) → ném `IllegalStateException`

**Kết quả**: tất cả pass.

---

## 1.2 — State transition thuần logic (chưa network)

### File đã viết

**`RaftMessages.java`** — 4 record RPC theo đúng mục 4.2 đặc tả:
- `RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm)`
- `RequestVoteResponse(term, voteGranted)`
- `AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)`
- `AppendEntriesResponse(term, success, matchIndex)`

**`RaftMessageHandler.java`** — 2 hàm thuần:

`handleRequestVote(state, req)` — 4 bước:
1. Term request cũ hơn `currentTerm` → reject ngay, không mutate gì.
2. Term mới hơn → cập nhật `currentTerm`, reset `votedFor = null`, lùi về `FOLLOWER`.
3. Tính `candidateLogUpToDate` (Election Restriction, Raft §5.4.1): so `lastLogTerm` trước, nếu bằng nhau mới so `lastLogIndex`.
4. Grant vote nếu (`votedFor == null` HOẶC `votedFor.equals(candidateId)` — case idempotent khi request bị gửi lại) VÀ log candidate đủ mới.

`handleAppendEntries(state, req)` — 5 bước:
1. Term cũ hơn → reject, log không đổi.
2. Term `>=` currentTerm (chú ý `>=` chứ không phải `>` — để candidate cùng term với 1 leader vừa thắng cử cũng phải lùi về follower ngay) → cập nhật `currentTerm`, lùi `FOLLOWER`.
3. Check `prevLogIndex/prevLogTerm` khớp với `termAt(prevLogIndex)` của mình không → không khớp thì reject (leader sẽ tự giảm `nextIndex` retry).
4. Khớp → lặp `entries[]`, gọi `appendOrOverwrite` cho từng cái.
5. Cập nhật `commitIndex = min(leaderCommit, lastNewIndex)` — không bao giờ vượt quá log thực có của mình.

### Test — `RaftMessageHandlerTest.java`

15 test case, gồm các case kinh điển theo hướng dẫn gốc:
- Vote: term cũ hơn reject; term mới hơn cập nhật + lùi follower; log candidate ngắn/cũ hơn reject (2 biến thể: ngắn hơn thẳng, và dài hơn nhưng term cũ hơn); log mới bằng thì grant; đã vote người khác thì reject người thứ 2; gửi lại request của đúng người đã vote thì vẫn grant (idempotent)
- AppendEntries: term cũ hơn reject, log không đổi; **term bằng nhau vẫn phải lùi CANDIDATE về FOLLOWER** (test riêng cho chỗ `>=`); `prevLogIndex` không tồn tại → reject; `prevLogTerm` khác → reject; conflicting entry → truncate rồi ghi đè; heartbeat rỗng vẫn cập nhật `commitIndex`; **`commitIndex` không được vượt quá log thực có** dù `leaderCommit` cao hơn nhiều; gửi lại entry cũ là idempotent

**Kết quả**: 14/15 pass ngay từ đầu. 1 test (`appendEntries_commitIndexKhongDuocVuotQuaLogThucCo`) fail do **lỗi trong chính test**, không phải lỗi logic `handleAppendEntries` — test set `prevLogIndex=0` (nghĩa là "leader tưởng follower log rỗng") trong khi `state` giả lập follower đã có sẵn entry index=1, dữ liệu không nhất quán. Sửa lại `prevLogIndex=1, prevLogTerm=1` cho khớp thực tế → cần chạy lại để xác nhận pass (bước cuối chưa có kết quả).

---

## 1.3 — Election timer + heartbeat (đơn luồng, chưa network)

### File đã viết

**`RaftTransport.java`** — interface trừu tượng cho việc gửi RPC giữa các node:
```java
RequestVoteResponse sendRequestVote(String targetNodeId, RequestVoteRequest req);
AppendEntriesResponse sendAppendEntries(String targetNodeId, AppendEntriesRequest req);
```
Thiết kế để Phase 2/3 chỉ cần viết thêm 1 implementation dùng Netty thật, không phải sửa logic election/heartbeat.

**`InJvmRaftTransport.java`** — bản giả lập trong 1 JVM, dùng `Map<String, RaftNode>`, gọi thẳng hàm xử lý của node đích thay vì qua mạng thật.

**`RaftNode.java`** — bọc quanh `RaftState` + `RaftMessageHandler`, thêm phần chủ động theo thời gian:
- `resetElectionTimer()` — huỷ `electionTask` cũ (nếu có), đặt lịch mới sau `random(150,300)`ms.
- `onElectionTimeout()` — tăng `currentTerm`, chuyển `CANDIDATE`, tự vote cho mình, đặt lại election timer (phòng split vote), rồi gọi `runElection()`.
- `runElection()` — gửi `RequestVote` tuần tự tới từng peer, đếm phiếu, luôn re-check `nodeState == CANDIDATE` và `currentTerm` chưa đổi trước khi tuyên bố thắng (tránh 2 leader cùng lúc do race condition), thắng quá bán thì `becomeLeader()`.
- `becomeLeader()` — huỷ election timer, khởi động `heartbeatTask` lặp mỗi 50ms.
- `sendHeartbeatToAll()` — gửi `AppendEntries` rỗng tới peer; nếu response mang term cao hơn → tự lùi về `FOLLOWER`, huỷ heartbeat, reset election timer.
- `onReceiveRequestVote`/`onReceiveAppendEntries` — gọi vào `RaftMessageHandler`, rồi tự `resetElectionTimer()` nếu request hợp lệ (vote granted, hoặc AppendEntries success) — đây là chỗ nối "logic thuần" (1.2) với "side effect theo thời gian" (1.3).
- Toàn bộ thao tác đọc/ghi `state` bọc `synchronized(this)`; `runElection()`/gọi transport được đặt **ngoài** khối synchronized để tránh deadlock giữa 2 node gọi lẫn nhau.

## Việc cần làm tiếp (đã cập nhật — xem mục Phase 2 bên dưới)
Sau khi Phase 1.1 → 1.2 → 1.3 đều pass ổn định, chuyển sang **Phase 2** (`rpc-transport`: Netty wire protocol thật) theo đúng nguyên tắc "đừng gắn Netty vào Raft trước khi logic thuần đã đúng" của roadmap.

---

# Tóm tắt tiến độ — Phase 2: `rpc-transport`

Package gốc: `com.nganhcc.orchestration.rpctransport`
Vị trí: `rpc-transport/src/main/java/...` và `rpc-transport/src/test/java/...`

## Quyết định thiết kế chốt khi triển khai (spec gốc để ngỏ)

- **Quy ước `totalLength`**: không tính 4 byte của chính nó — chỉ tính `messageType(1)+requestId(8)+epoch(8)+payload(N)`. Đã ghi bổ sung vào `SPEC.md` mục 5.1. Ánh xạ sang tham số `LengthFieldBasedFrameDecoder`: `lengthFieldOffset=0, lengthFieldLength=4, lengthAdjustment=0, initialBytesToStrip=4`.
- **`maxFrameLength=1MB`** cho `LengthFieldBasedFrameDecoder` — chặn client khai `totalLength` giả mạo cực lớn (DoS), chưa biết payload LLM thực tế lớn nhất nên tạm chọn 1MB, có thể chỉnh lại ở Phase 5.

## File đã viết

**Tầng thuần (không phụ thuộc Netty):**
- `FrameMessage.java` — record, override `equals`/`hashCode` thủ công (record mặc định so `byte[]` theo reference).
- `AssignStepPayload.java` — cấu trúc lồng cho payload của `ASSIGN_STEP` (`traceId`, `jobId`, `stepId`, `payload`).
- `FrameCodec.java` — nơi duy nhất chứa logic đọc/ghi byte thật: `encodeFrame`/`decodeFrame` (tầng ngoài), `decodeBody` (dùng khi length field đã bị Netty strip), `encodeAssignStepPayload`/`decodeAssignStepPayload` (tầng trong).

**Tầng Netty (lớp vỏ mỏng, gọi vào tầng thuần):**
- `NettyFrameDecoder.java` (`extends ByteToMessageDecoder`) — `ByteBuf` → `byte[]` → gọi `FrameCodec.decodeBody`.
- `NettyFrameEncoder.java` (`extends MessageToByteEncoder<FrameMessage>`) — gọi `FrameCodec.encodeFrame` → ghi `ByteBuf`.
- `RpcHandler.java` (`extends SimpleChannelInboundHandler<FrameMessage>`) — trạm cuối pipeline, `switch` theo `messageType`; `HEARTBEAT→ACK` đã xử lý, `ASSIGN_STEP` mới log ra (chưa gọi `LlmClient`, để Phase 5); có override `exceptionCaught`.
- `RpcChannelInitializer.java` — định nghĩa pipeline cho mỗi `Channel` mới: `LengthFieldBasedFrameDecoder → NettyFrameDecoder → NettyFrameEncoder → RpcHandler`.

**Chưa viết:** `EchoServer`/`EchoClient` (dùng `ServerBootstrap`/`Bootstrap` thật, bước 2.5) — cần `FrameCodecTest` + `NettyPipelineTest` pass ổn định trước.

## Test

**`FrameCodecTest.java`** (tầng thuần, không cần Netty) — các nhóm case: round-trip `HEARTBEAT`/`ASSIGN_STEP` với assert byte-level (`totalLength` đúng giá trị tính tay, không chỉ so object); `jobId` tiếng Việt (byte length ≠ char length); `jobIdLen` gần biên 32768 (tránh đọc `short` có dấu ra số âm, dùng `Short.toUnsignedInt`); `jobId` vượt 65535 byte → throw; payload lớn ~100KB không tràn; `totalLength` khai man → throw.

**`NettyPipelineTest.java`** (dùng `EmbeddedChannel`, không mở socket thật) — frame đến đủ 1 lần; frame bị chia làm 2 gói TCP (verify decoder tự chờ, không đẩy frame dở dang — case quan trọng nhất trong nhóm); 2 frame dính liền trong 1 gói (verify tách đúng từng frame); encode rồi decode ngược qua cả 2 encoder trong cùng pipeline.

**Kết quả:** chưa chạy được — vướng lỗi cấu hình Gradle, đang xử lý (xem mục dưới). Code test đã viết xong, chưa có lần chạy pass nào được xác nhận.

## Lỗi Gradle đã gặp khi chạy `./gradlew :rpc-transport:test`

1. **Cú pháp Groovy DSL trong file `.kts`**: project dùng Kotlin DSL (`build.gradle.kts`, đồng bộ với các module khác như `orchestrator`), nhưng bản `build.gradle` soạn ban đầu theo cú pháp Groovy (`id 'java'`, `implementation '...'`). Kotlin DSL cần ngoặc tròn cho function call: `id("java")`, `implementation("...")`, `tasks.test { }` (không viết tắt `test { }` như Groovy). Đã sửa.
2. **Thiếu `junit-platform-launcher`**: Gradle 9.6.1 không tự kéo dependency này vào classpath test nữa (khác các bản Gradle cũ) — lỗi `Failed to load JUnit Platform`. Sửa bằng cách thêm `testRuntimeOnly("org.junit.platform:junit-platform-launcher")` vào `dependencies`. Cân nhắc gom dòng này vào `subprojects { }` ở `build.gradle.kts` gốc sau này để không lặp lại ở từng module.

**Kết quả sau 2 lần sửa:** chưa có xác nhận build pass — cần chạy lại `./gradlew :rpc-transport:test` sau khi áp cả 2 sửa trên.

## Việc cần làm tiếp
1. Chạy lại `./gradlew :rpc-transport:test`, xác nhận `FrameCodecTest` + `NettyPipelineTest` pass toàn bộ.
2. ~~Viết `EchoServer`/`EchoClient` (bước 2.5)~~ — đã viết, xem mục 2.5 bên dưới. Còn thiếu: chạy thử thật và xác nhận kết quả.
3. Sau khi Phase 2 xong, sang **Phase 3** (ghép `raft-core` vào network thật qua `rpc-transport`).

---

## 2.5 — Echo server/client thật qua TCP

### File đã viết

- **`EchoServer.java`** — đóng vai worker. Dùng `ServerBootstrap`, tách `bossGroup` (1 thread, chỉ accept connection) và `workerGroup` (xử lý I/O thật cho từng connection đã accept). `childHandler` gắn `RpcChannelInitializer(new RpcHandler())` cho mỗi client kết nối tới. Nhận port qua `args[0]`, mặc định `9000`.

- **`EchoClient.java`** — đóng vai orchestrator. Dùng `Bootstrap`, connect tới server, gửi 5 `HEARTBEAT` liên tiếp (`requestId` tăng dần 1→5, cách nhau 200ms để log dễ đọc), mỗi lần gửi ghi lại `System.nanoTime()` vào `ClientAckHandler` để tính round-trip khi `ACK` về. Nhận `host`/`port` qua `args`.

- **`ClientAckHandler.java`** — `SimpleChannelInboundHandler<FrameMessage>` riêng cho phía client (khác `RpcHandler` phía server, vì vai trò ngược nhau: server nhận `HEARTBEAT` trả `ACK`, client nhận `ACK` để đo thời gian). Lưu `Map<requestId, sentTimestamp>` bằng `ConcurrentHashMap`, khi nhận `ACK` thì `remove` theo `requestId`, tính `System.nanoTime() - sentAt`, log ra ms. Có override `exceptionCaught`.

### Sửa lại file cũ

- **`RpcChannelInitializer.java`** — đổi kiểu tham số constructor từ `RpcHandler businessHandler` thành `ChannelHandler businessHandler` (interface chung của Netty), để dùng chung 1 class này cho cả server (`RpcHandler`) lẫn client (`ClientAckHandler`), thay vì phải viết thêm class factory riêng cho từng bên.

### Cấu hình Gradle cần thêm (chưa làm)

Cần thêm plugin `application` vào `rpc-transport/build.gradle.kts` để chạy `main()` của `EchoServer`/`EchoClient` trực tiếp bằng `./gradlew run`. Tạm thời có thể chạy qua IDE (Run trực tiếp file) để tránh vướng thêm cấu hình trong lúc demo nhanh.

### Kỳ vọng khi chạy thử

Terminal 1 (`EchoServer`, port 9000) → terminal 2 (`EchoClient`, connect `localhost:9000`) → server in 5 dòng nhận `HEARTBEAT`, client in 5 dòng `ACK requestId=... round-trip = X.XX ms`. Số latency này là baseline dùng so sánh với Kafka transport ở Phase 7, ghi vào README ở Phase 8.

**Trạng thái: chưa chạy thử thật, chưa có số liệu latency xác nhận.** Nếu số bất thường (hàng trăm ms trên localhost), nghi vấn đầu tiên: thiếu `flush()` hoặc `Thread.sleep` chặn nhầm chỗ trong `EchoClient`.
