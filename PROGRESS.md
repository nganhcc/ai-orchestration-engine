# Tóm tắt tiến độ — Phase 4 bắt đầu: fencing token / epoch

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
