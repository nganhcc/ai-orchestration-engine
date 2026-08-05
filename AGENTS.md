# Commands

- Build toàn bộ: `./gradlew build`
- Test 1 module: `./gradlew :raft-core:test`, `./gradlew :rpc-transport:test`, `./gradlew :orchestrator:test`
- Test 1 class: `./gradlew :raft-core:test --tests "RaftLogTest"`
- Chạy demo cluster 3 node Raft thật: `docker compose up -d` rồi `docker compose logs -f orchestrator-a orchestrator-b orchestrator-c`
- Giả lập kill leader: `docker kill <container>` — giả lập partition (không kill): `docker network disconnect ai-net <container>`

# Tooling

- Java 21, Gradle Kotlin DSL (`.kts`) — **không** dùng cú pháp Groovy. Viết `id("java")`, `implementation("...")`, không viết `id 'java'`.
- Gradle 9.6.1 không tự kéo `junit-platform-launcher` vào classpath test. Module test nào thiếu dòng này sẽ lỗi `Failed to load JUnit Platform`:
  ```kotlin
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  ```

# Quy ước đã chốt khi implement (không nằm rõ trong code, spec để ngỏ)

- **Frame `totalLength`** (rpc-transport): không tính 4 byte của chính nó — chỉ tính `messageType(1)+requestId(8)+epoch(8)+payload(N)`.
- **`LengthFieldBasedFrameDecoder`**: `lengthFieldOffset=0, lengthFieldLength=4, lengthAdjustment=0, initialBytesToStrip=4`, `maxFrameLength=1MB` (tạm thời — chưa biết payload LLM lớn nhất thực tế, coi lại ở Phase 5).
- **Epoch**: `epoch = currentTerm` mỗi lần leader thắng election. Step-down reset `epoch = 0`.

# Gotcha đã tốn thời gian debug — đừng lặp lại

- `record` trong Java so `byte[]` theo reference, không theo nội dung. `FrameMessage`/`AssignStepPayload` phải override `equals`/`hashCode` thủ công.
- `jobId`/`stepId` chứa tiếng Việt: `.getBytes(UTF_8).length` khác `.length()`. Luôn dùng byte length khi ghi length field, không dùng số ký tự.
- `jobIdLen` dùng kiểu `short` có dấu trong Java — đọc gần biên 32768 mà quên `Short.toUnsignedInt` sẽ ra số âm → `NegativeArraySizeException`.
- Test `AppendEntries`/`RequestVote`: dựng `state` giả lập phải khớp `prevLogIndex`/`prevLogTerm` thực tế của log, không phải số bất kỳ — dữ liệu test không nhất quán từng gây fail nhầm (lỗi ở test, không phải ở logic).

# Không được đụng / cẩn thận khi sửa

- `raft-core/.../RaftLog.java`, `RaftMessageHandler.java` — đã test kỹ các case kinh điển của Raft (term cũ, log conflict, split vote, commitIndex vượt log thực có...). Sửa gì cũng phải chạy lại `RaftLogTest` + `RaftMessageHandlerTest` + `RaftNettyClusterIntegrationTest` trước khi báo xong.
- Field order trong Netty wire protocol (`rpc-transport`) — đây là custom protocol tự thiết kế, đổi order sẽ break wire compatibility với node khác đang chạy.
- Toàn bộ method trong `RaftLog`/`RaftState` là `synchronized` (election timer thread + RPC handler thread cùng đụng vào) — đừng bỏ khi refactor.

# Nguyên tắc làm việc của dự án này

- Viết logic thuần (test được, không cần network/DB thật) trước, chỉ gắn hạ tầng thật (Netty, Raft cluster, Kafka) sau khi logic pass test ổn định. Đừng gắn Netty vào Raft trước khi logic thuần đã đúng — không phân biệt được bug do logic hay do network.
- Trạng thái phase hiện tại và việc cần làm tiếp: xem `PROGRESS.md`. File này (AGENT.md) không lặp lại trạng thái đó vì nó đổi liên tục.
- Roadmap đầy đủ và lý do chọn kỹ thuật: xem `PLAN.md` / `SPEC.md`.

# Definition of done

1. `./gradlew :<module>:test` pass cho module vừa sửa
2. Nếu đụng `raft-core` hoặc `rpc-transport`: chạy thêm integration test liên quan (xem mục "Không được đụng")
3. Không thêm dependency mới vào `build.gradle.kts` nếu chưa hỏi
