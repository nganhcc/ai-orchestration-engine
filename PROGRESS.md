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

## Việc cần làm tiếp
1. Sau khi Phase 1.1 → 1.2 → 1.3 đều pass ổn định, mới chuyển sang **Phase 2** (`rpc-transport`: Netty wire protocol thật) theo đúng nguyên tắc "đừng gắn Netty vào Raft trước khi logic thuần đã đúng" của roadmap.
