# Hướng dẫn triển khai từng bước — Distributed AI Orchestration Engine

Tài liệu này chia roadmap 8 tuần trong đặc tả kỹ thuật thành các bước nhỏ, có thứ tự phụ thuộc rõ ràng và điểm kiểm tra (checkpoint) sau mỗi bước. Nguyên tắc xuyên suốt: viết logic thuần (pure function, test được không cần network/DB) trước, chỉ gắn hạ tầng thật (Netty, Raft cluster, Kafka) sau khi logic đã pass test ổn định.

---

## Phase 0 — Khung dự án (1–2 ngày)

**Mục tiêu:** multi-module Gradle build chạy được, docker-compose khởi động Postgres + Redis.

Việc cần làm:

1. `gradle init` tạo multi-module: `raft-core`, `rpc-transport`, `orchestrator`, `worker`, `common` — module `common` chứa `LogEntry`, `JobCommand`, các DTO dùng chung, tránh phụ thuộc vòng giữa các module khác.
2. Viết `docker-compose.yml` chỉ với Postgres + Redis trước (Kafka/Spark thêm sau ở Phase 7). Ít moving part hơn giúp debug nhanh hơn ở giai đoạn đầu.
3. Chạy Flyway hoặc Liquibase (chọn 1) để quản lý schema — dùng schema SQL ở mục 7.1 của đặc tả kỹ thuật, migrate thử.
4. Commit sớm, CI đơn giản (GitHub Actions: `./gradlew build`) để không tự phá vỡ build giữa chừng.

**Kiểm tra xong:** `docker compose up -d && ./gradlew build` chạy sạch, connect được Postgres bằng psql.

---

## Phase 1 — `raft-core`: chỉ phần đơn giản nhất trước (1 tuần)

Đây là phần dễ viết sai nhất nếu làm một lèo. Chia nhỏ theo 3 bước:

### 1.1 Data model + in-memory log (không network)

- `RaftState`, `LogEntry` như mục 4.1 của đặc tả kỹ thuật.
- Viết `RaftLog` class thuần in-memory: `append`, `getEntry(index)`, `truncateFrom(index)` (dùng khi conflict), `lastIndex()`, `lastTerm()`.
- Test unit trước khi đụng network: truncate khi có conflicting entry, append idempotent khi gửi lại entry cũ.

### 1.2 State transition thuần logic (không RPC thật)

- Viết hàm xử lý `RequestVote` và `AppendEntries` như hàm thuần nhận input trả output, chưa gắn Netty:

  ```java
  RequestVoteResponse handleRequestVote(RaftState state, RequestVoteRequest req);
  AppendEntriesResponse handleAppendEntries(RaftState state, AppendEntriesRequest req);
  ```

- Lý do tách: có thể viết test case cho toàn bộ edge case của Raft (term cũ hơn, log không match, split vote...) mà không cần dựng cluster thật, không cần network — đây là cách thực tế duy nhất để bắt bug Raft sớm.
- Tối thiểu cần test các case kinh điển:
  - Candidate với term cũ hơn → reject.
  - Log của candidate ngắn hơn/cũ hơn leader hiện tại → reject (Raft election restriction).
  - `AppendEntries` có `prevLogIndex/prevLogTerm` không khớp → reject, leader phải giảm `nextIndex` retry.
  - Conflicting entry cùng index khác term → truncate rồi ghi đè.

### 1.3 Election timer + heartbeat (đơn luồng, chưa network)

- Dùng `ScheduledExecutorService`, random timeout 150–300ms.
- Chạy thử với 3 instance `RaftState` trong cùng 1 JVM, gọi hàm thuần ở 1.2 trực tiếp với nhau (không qua network) để verify: 1 leader được bầu, heartbeat giữ được leadership, nếu leader "im lặng" (giả lập bằng cách không gọi heartbeat) thì follower bầu lại.

Chỉ sau khi bước 1.1–1.3 pass test ổn định mới sang Phase 2 (gắn network thật). Đừng gắn Netty vào Raft trước khi logic thuần đã đúng — sẽ không phân biệt được bug do logic hay do network.

---

## Phase 2 — `rpc-transport`: Netty wire protocol (1 tuần)

**Mục tiêu:** frame format ở mục 5.1 của đặc tả kỹ thuật chạy đúng, độc lập với Raft/orchestrator.

1. Viết encoder/decoder cho frame length-prefixed: dùng `LengthFieldBasedFrameDecoder` của Netty cho phần đọc, tự viết `ByteToMessageDecoder`/`MessageToByteEncoder` cho phần parse `messageType/requestId/epoch/payload`.
2. Viết test round-trip: encode → decode → so sánh object gốc, bao gồm case payload rỗng, payload lớn (test frame length 4 bytes không tràn).
3. Netty echo server/client đơn giản: gửi `HEARTBEAT`, nhận `ACK`.
4. Chưa cần epoch check ở bước này — đó là việc của Phase 4. Ở đây chỉ cần transport hoạt động đúng.

**Kiểm tra xong:** 2 process Java riêng biệt (client/server) trao đổi frame qua TCP thật, có test đo round-trip latency thô để dùng làm baseline cho metric sau này.

---

## Phase 3 — Ghép `raft-core` vào network thật (1 tuần)

- Dùng `rpc-transport` (hoặc một RPC nội bộ đơn giản khác nếu muốn tách riêng giao tiếp Raft-node với giao tiếp orchestrator↔worker — đặc tả không bắt buộc dùng chung transport) để 3 tiến trình Raft thật sự gọi nhau qua network.
- Docker Compose thêm 3 node orchestrator (chưa có REST API, chỉ chạy Raft).
- Demo tối thiểu: `docker logs` cho thấy 1 node thành leader, kill leader bằng `docker kill`, quan sát failover < 1s, log thời gian từng giai đoạn (election timeout → vote → majority) đúng mục 11.2 của đặc tả kỹ thuật.
- Đây là điểm dừng an toàn đầu tiên: nếu hết thời gian sau mốc này, vẫn có một demo Raft 3-node failover chạy được, đủ để trình bày trong phỏng vấn dù chưa có phần AI/document xử lý.

---

## Phase 4 — Epoch/fencing + snapshot (3–5 ngày)

1. Thêm `epoch` field vào mọi RPC (đã có sẵn chỗ trong frame ở Phase 2) — gắn logic: mỗi lần thắng election, `epoch = currentTerm`, broadcast trước khi assign step.
2. Worker lưu `highestEpochSeen`, reject nếu epoch cũ hơn — trả `STALE_LEADER_REJECT` (`0x05`).
3. Postgres: cột `leader_epoch`, update có điều kiện `WHERE leader_epoch <= :epoch`.
4. Demo bắt buộc trước khi đi tiếp: kịch bản 2 trong mục 13 của đặc tả kỹ thuật — `docker network disconnect` leader (không kill), verify leader cũ vẫn tưởng mình là leader nhưng mọi write đều bị reject vì epoch cũ. Đây là phần "ăn điểm" nhất khi phỏng vấn, đầu tư kỹ.
5. Snapshot/log compaction (mục 4.4 của đặc tả kỹ thuật): làm sau cùng trong phase này, có thể để tuần sau nếu thiếu thời gian — quan trọng cho scale nhưng không phải thứ demo trực tiếp trong phỏng vấn bằng epoch/fencing.

---

## Phase 5 — Orchestrator + Worker end-to-end, WAL/resume, outbox (1.5–2 tuần)

Đây là phase dài nhất, chia nhỏ theo thứ tự:

1. Single-node trước, mock LLM trước (đừng gọi LLM thật ngay — tốn tiền và chậm debug). Orchestrator nhận `POST /api/batches`, ghi Postgres, gửi `AssignStep` qua RPC tới 1 worker, worker gọi `LlmClient` mock (sleep + trả JSON giả), `MarkStepDone`.
2. Dedup table ở worker (`stepId -> {IN_PROGRESS, DONE}`) — test bằng cách cố tình gửi lại `AssignStep` với `stepId` đã `DONE`, verify không chạy lại LLM call.
3. Outbox pattern: trong 1 transaction, update `job_step.status=DONE` + insert `outbox`. Viết poller riêng (`@Scheduled` đơn giản là đủ, chưa cần phức tạp) đọc `outbox WHERE NOT published`, publish sang Redis cache invalidation, set `published=true`.
4. Resume flow sau crash (mục 7.3 của đặc tả kỹ thuật): kill worker giữa batch, restart orchestrator/worker, verify step `IN_PROGRESS` quá `staleness_threshold` được reassign đúng, step `DONE` không bao giờ chạy lại. Đây là metric "% step chạy lại — mục tiêu 0%" ở mục 18, đo và ghi lại số thật.
5. Backpressure (mục 5.3 của đặc tả kỹ thuật): credit-based flow control — làm sau khi correctness (bước 1–4) đã ổn, đừng tối ưu performance trước khi đúng.

**Idempotency-Key ở API** (`POST /api/batches`): làm cùng lúc với bước 1, đơn giản (unique constraint + trả lại `batchId` cũ nếu trùng key).

---

## Phase 6 — DAG scheduling + priority scheduling (1 tuần)

1. `depends_on UUID[]`, state machine chuyển `BLOCKED → PENDING` khi mọi dependency `DONE` — viết như 1 hàm thuần test được độc lập trước (giống cách làm Raft ở Phase 1), input là job_step graph + event `MarkStepDone`, output là step nào chuyển trạng thái.
2. Cycle detection khi `SubmitDagStep` — dùng DFS/topological sort đơn giản, reject rõ ràng nếu có cycle.
3. Priority: deficit round robin giữa các batch `RUNNING` — đây là phần dễ overengineer, giữ đơn giản đúng như đặc tả kỹ thuật ("không cần scheduler phức tạp kiểu Kubernetes").
4. Demo: batch DAG `OCR → extract → validate` chạy đúng thứ tự, 2 batch priority khác nhau chạy xen kẽ không đói nhau.

---

## Phase 7 — Kafka transport, circuit breaker, Spark analytics (1–1.5 tuần)

Nếu quỹ thời gian hạn chế, dừng ở bất cứ đâu trong danh sách này vẫn cho demo tốt. Thứ tự ưu tiên:

1. **Circuit breaker cho LLM** (mục 10.2 của đặc tả kỹ thuật) — tương đối rẻ để làm (1 state machine `CLOSED/OPEN/HALF_OPEN`), giá trị demo cao, nên làm trước Kafka/Spark.
2. **Cache layer + hit rate metric** — rẻ, làm cùng lúc circuit breaker.
3. **Kafka transport** — chỉ làm nếu Phase 1–6 đã ổn và còn ít nhất 1–1.5 tuần. Nếu thiếu thời gian, ghi rõ trong README: "thiết kế đã tính đến Kafka transport (xem mục 6 của đặc tả kỹ thuật), chưa kịp implement trong phạm vi demo — đây là hướng mở rộng." Người phỏng vấn tôn trọng việc biết giới hạn hơn là code dở dang.
4. **Spark Structured Streaming** (mục 15 của đặc tả kỹ thuật) — cắt cuối cùng nếu thiếu thời gian, giá trị demo thấp nhất so với công sức bỏ ra (cần học Spark riêng nếu chưa quen).

---

## Phase 8 — Chaos test mở rộng + đo metric + tài liệu (3–5 ngày)

- Chạy đủ 3 kịch bản mục 13 của đặc tả kỹ thuật, ghi số thật (không phải số ước tính) vào README: failover time, % step rerun, cache hit rate, circuit breaker OPEN duration.
- Quay video demo ngắn (2–3 phút): submit batch → kill/partition leader → hệ thống tự phục hồi → show log epoch reject.
- Viết README theo thứ tự: vấn đề → kiến trúc → quyết định kỹ thuật quan trọng (kèm lý do, đặc biệt outbox vs 2PC, fencing token) → cách chạy demo → metric đo được.

---

## Checklist tối giản nếu chỉ có 4–5 tuần thay vì 8

Nếu deadline gấp hơn dự kiến, đây là "lõi cứng" đủ để có một câu chuyện phỏng vấn mạnh mà không cần Kafka/Spark/DAG phức tạp:

- [ ] Raft 3-node, election + log replication (Phase 1 + 3)
- [ ] Epoch/fencing, demo network partition không kill (Phase 4)
- [ ] Orchestrator/worker single pipeline (không cần DAG nhiều bước, 1 step cũng được), dedup, outbox (Phase 5)
- [ ] Resume sau crash, đo % step rerun = 0%
- [ ] Chaos test + README

Đây vẫn là một dự án hệ thống phân tán nghiêm túc, nhỏ hơn phạm vi đầy đủ của đặc tả kỹ thuật, nhưng correctness và độ sâu không giảm.

---

## Bước tiếp theo

Bắt đầu từ Phase 0 + 1.1/1.2 (data model + hàm thuần cho Raft) là hợp lý nhất — đây là phần vừa nền tảng vừa test được độc lập sớm nhất.
