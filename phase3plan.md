# Phase 3 — Ghép `raft-core` vào network thật

## Summary
- Mục tiêu: chạy 3 tiến trình orchestrator thật qua Netty TCP, bầu đúng 1 Raft leader, heartbeat giữ leadership, `docker compose kill` leader thì cụm bầu leader mới trong < 1s.
- Nền hiện tại đã xanh: `./gradlew :raft-core:test :rpc-transport:test` pass.
- Phase này chỉ làm Raft election/heartbeat qua network; chưa làm REST API, worker assignment, epoch/fencing, snapshot, hay log replication cho job command.

## Key Changes
- Dùng lại `rpc-transport` frame protocol, thêm message type Raft:
  - `0x10=RAFT_REQUEST_VOTE`
  - `0x11=RAFT_REQUEST_VOTE_RESPONSE`
  - `0x12=RAFT_APPEND_ENTRIES`
  - `0x13=RAFT_APPEND_ENTRIES_RESPONSE`
- Thêm Raft payload codec dạng binary trong orchestrator layer:
  - `RequestVote`: `term`, `candidateId`, `lastLogIndex`, `lastLogTerm`
  - `RequestVoteResponse`: `term`, `voteGranted`
  - `AppendEntries`: `term`, `leaderId`, `prevLogIndex`, `prevLogTerm`, `entries[]`, `leaderCommit`
  - `AppendEntriesResponse`: `term`, `success`, `matchIndex`
  - `FrameMessage.epoch = 0` cho Raft-internal RPC để không trộn nghĩa với fencing epoch của Phase 4.
- Thêm request/reply Netty client dùng `requestId -> CompletableFuture`, timeout 100ms; response trả cùng `requestId`.
- Thêm `RaftNettyTransport implements RaftTransport`, gọi TCP tới peer và block theo timeout hiện có của `RaftNode`.
- Thêm Netty Raft server handler: decode frame Raft, gọi `RaftNode.onReceiveRequestVote` / `onReceiveAppendEntries`, encode response.
- Bổ sung observability nhẹ vào `raft-core`: constructor overload hoặc listener no-op để log `election.timeout`, `vote.response`, `leader.elected`, `heartbeat`, `step_down`, kèm `term`, `nodeId`, `peerId`, `elapsedMs`.

## Orchestrator + Docker
- `orchestrator` bootstrap Raft bằng env:
  - `RAFT_NODE_ID`
  - `RAFT_PORT`
  - `RAFT_PEERS`, format: `nodeB=host:port,nodeC=host:port`
  - `SPRING_MAIN_WEB_APPLICATION_TYPE=none` cho Phase 3 raft-only mode.
- Docker Compose thêm 3 service:
  - `orchestrator-a`, `orchestrator-b`, `orchestrator-c`
  - cùng image từ `:orchestrator:bootJar`
  - mỗi service expose nội bộ port `7000`, map host thành `7001/7002/7003` để debug.
- Thêm Dockerfile orchestrator chạy jar Spring Boot.
- Cập nhật README/PROGRESS với lệnh demo:
  - `./gradlew :orchestrator:bootJar`
  - `docker compose up -d --build postgres redis orchestrator-a orchestrator-b orchestrator-c`
  - `docker compose logs -f orchestrator-a orchestrator-b orchestrator-c`
  - xác định leader qua log `raft.leader.elected`
  - `docker compose kill <leader-service>`
  - quan sát leader mới và log elapsed failover < 1s.

## Test Plan
- Giữ xanh regression:
  - `./gradlew :raft-core:test :rpc-transport:test`
- Thêm unit test cho Raft binary codec:
  - round-trip đủ 4 RPC
  - `nodeId` UTF-8
  - `entries[]` rỗng cho heartbeat
  - `entries[]` có ít nhất 1 `LogEntry`
  - malformed length/payload ném exception rõ ràng.
- Thêm integration test TCP trong JVM:
  - start 3 `RaftNode` + 3 Netty Raft server trên localhost port động
  - verify đúng 1 leader
  - stop leader node/server
  - verify 2 node còn lại bầu đúng 1 leader mới, term tăng, trong < 1s.
- Manual Docker acceptance:
  - compose logs có đúng 1 leader ổn định trước kill
  - sau `docker compose kill` leader, node mới log `leader.elected`
  - không có 2 leader cùng term trong log quan sát được.

## Assumptions
- Phase 3 ưu tiên demo failover ổn định hơn mở rộng replication.
- Transport Raft dùng custom Netty frame hiện có, không thêm gRPC/Thrift.
- PostgreSQL/Redis vẫn có trong Compose vì đã thuộc Phase 0, nhưng Phase 3 không đọc/ghi business data.
- Failover < 1s tính từ lúc leader container bị kill xong đến lúc leader mới được bầu, không tính thời gian build/start container.
