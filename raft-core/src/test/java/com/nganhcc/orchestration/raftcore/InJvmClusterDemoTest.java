package com.nganhcc.orchestration.raftcore;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class InJvmClusterDemoTest {

    @Test
    void electsExactlyOneLeader() throws InterruptedException {
        Map<String, RaftNode> nodes = new HashMap<>();
        InJvmRaftTransport transport = new InJvmRaftTransport(nodes);

        RaftNode a = new RaftNode("A", Set.of("B", "C"), transport);
        RaftNode b = new RaftNode("B", Set.of("A", "C"), transport);
        RaftNode c = new RaftNode("C", Set.of("A", "B"), transport);
        nodes.put("A", a); nodes.put("B", b); nodes.put("C", c);

        a.start(); b.start(); c.start();
        Thread.sleep(1000); // đợi election ổn định

        long leaderCount = List.of(a, b, c).stream()
            .filter(n -> n.state().nodeState == NodeState.LEADER)
            .count();
        assertEquals(1, leaderCount, "Phải có đúng 1 leader");

        RaftNode leader = List.of(a, b, c).stream()
            .filter(n -> n.state().nodeState == NodeState.LEADER)
            .findFirst().orElseThrow();
        assertEquals(leader.state().currentTerm, leader.state().epoch,
            "Epoch phải khớp term khi node trở thành leader");

        a.stop(); b.stop(); c.stop();
    }

    @Test
    void reElectsLeaderWhenCurrentLeaderStops() throws InterruptedException {
        Map<String, RaftNode> nodes = new HashMap<>();
        InJvmRaftTransport transport = new InJvmRaftTransport(nodes);

        RaftNode a = new RaftNode("A", Set.of("B", "C"), transport);
        RaftNode b = new RaftNode("B", Set.of("A", "C"), transport);
        RaftNode c = new RaftNode("C", Set.of("A", "B"), transport);
        nodes.put("A", a); nodes.put("B", b); nodes.put("C", c);

        a.start(); b.start(); c.start();
        Thread.sleep(1000);

        List<RaftNode> all = List.of(a, b, c);
        RaftNode oldLeader = all.stream()
            .filter(n -> n.state().nodeState == NodeState.LEADER)
            .findFirst().orElseThrow(() -> new AssertionError("Chưa có leader nào được bầu"));

        long oldTerm = oldLeader.state().currentTerm;
        oldLeader.stop(); // giả lập leader "im lặng" hẳn (crash)

        List<RaftNode> remaining = all.stream()
            .filter(n -> n != oldLeader)
            .toList();

        Thread.sleep(1000); // đợi đủ qua election timeout (150-300ms) + vài vòng nếu split vote

        long newLeaderCount = remaining.stream()
            .filter(n -> n.state().nodeState == NodeState.LEADER)
            .count();
        assertEquals(1, newLeaderCount, "Phải có đúng 1 leader mới trong 2 node còn lại");

        RaftNode newLeader = remaining.stream()
            .filter(n -> n.state().nodeState == NodeState.LEADER)
            .findFirst().orElseThrow();
        assertTrue(newLeader.state().currentTerm > oldTerm, "Term phải tăng sau khi bầu lại");
        assertEquals(newLeader.state().currentTerm, newLeader.state().epoch,
            "Epoch của leader mới phải khớp term hiện tại");

        remaining.forEach(RaftNode::stop);
    }

    @Test
    void leaderStaysStableWhileHeartbeatKeepsRunning() throws InterruptedException {
        Map<String, RaftNode> nodes = new HashMap<>();
        InJvmRaftTransport transport = new InJvmRaftTransport(nodes);

        RaftNode a = new RaftNode("A", Set.of("B", "C"), transport);
        RaftNode b = new RaftNode("B", Set.of("A", "C"), transport);
        RaftNode c = new RaftNode("C", Set.of("A", "B"), transport);
        nodes.put("A", a); nodes.put("B", b); nodes.put("C", c);

        a.start(); b.start(); c.start();
        Thread.sleep(500);

        List<RaftNode> all = List.of(a, b, c);
        String leaderIdBefore = all.stream()
            .filter(n -> n.state().nodeState == NodeState.LEADER)
            .findFirst().orElseThrow().state().selfId;
        long termBefore = nodes.get(leaderIdBefore).state().currentTerm;

        // để cluster chạy tiếp 2 giây, không can thiệp gì - heartbeat (50ms/lần) phải
        // liên tục reset election timer của follower, không ai được tự ứng cử giữa chừng
        Thread.sleep(2000);

        long leaderCountAfter = all.stream()
            .filter(n -> n.state().nodeState == NodeState.LEADER)
            .count();
        assertEquals(1, leaderCountAfter, "Vẫn phải đúng 1 leader, không được bầu lại vô cớ");

        String leaderIdAfter = all.stream()
            .filter(n -> n.state().nodeState == NodeState.LEADER)
            .findFirst().orElseThrow().state().selfId;
        assertEquals(leaderIdBefore, leaderIdAfter, "Leader không được đổi nếu không có sự cố gì");
        assertEquals(termBefore, nodes.get(leaderIdAfter).state().currentTerm, "Term không được tăng nếu leader vẫn sống bình thường");

        all.forEach(RaftNode::stop);
    }
    @Test
    void electsLeaderConsistentlyAcrossManyRuns() throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            Map<String, RaftNode> nodes = new HashMap<>();
            InJvmRaftTransport transport = new InJvmRaftTransport(nodes);

            RaftNode a = new RaftNode("A", Set.of("B", "C"), transport);
            RaftNode b = new RaftNode("B", Set.of("A", "C"), transport);
            RaftNode c = new RaftNode("C", Set.of("A", "B"), transport);
            nodes.put("A", a); nodes.put("B", b); nodes.put("C", c);

            a.start(); b.start(); c.start();
            Thread.sleep(800);

            long leaderCount = List.of(a, b, c).stream()
                .filter(n -> n.state().nodeState == NodeState.LEADER)
                .count();
            assertEquals(1, leaderCount, "Lần chạy thứ " + i + " phải có đúng 1 leader");

            a.stop(); b.stop(); c.stop();
        }
    }
}