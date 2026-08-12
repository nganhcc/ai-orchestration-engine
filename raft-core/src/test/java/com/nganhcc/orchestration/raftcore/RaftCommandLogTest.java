package com.nganhcc.orchestration.raftcore;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RaftCommandLogTest {

    @Test
    void propose_appendsReplicatesAndCommitsOpaqueCommand() throws Exception {
        Map<String, RaftNode> nodes = new HashMap<>();
        InJvmRaftTransport transport = new InJvmRaftTransport(nodes);
        RaftNode a = new RaftNode("A", Set.of("B", "C"), transport);
        RaftNode b = new RaftNode("B", Set.of("A", "C"), transport);
        RaftNode c = new RaftNode("C", Set.of("A", "B"), transport);
        nodes.put("A", a);
        nodes.put("B", b);
        nodes.put("C", c);

        try {
            a.start();
            b.start();
            c.start();

            RaftNode leader = awaitLeader(List.of(a, b, c));
            byte[] command = new byte[]{0, 1, 2, (byte) 0xff};
            RaftNode.CommitResult result = leader.propose(command);

            assertEquals(1L, result.logIndex());
            for (RaftNode node : List.of(a, b, c)) {
                LogEntry entry = node.state().log.getEntry(result.logIndex()).orElseThrow();
                assertArrayEquals(command, entry.command());
            }
            assertEquals(result.logIndex(), leader.state().commitIndex);
            assertEquals(result.logIndex(), leader.state().lastApplied);
        } finally {
            a.stop();
            b.stop();
            c.stop();
        }
    }

    @Test
    void propose_singleNodeCommitsWithoutPeer() {
        Map<String, RaftNode> nodes = new HashMap<>();
        RaftNode node = new RaftNode("A", Set.of(), new InJvmRaftTransport(nodes));
        nodes.put("A", node);
        node.state().currentTerm = 7;
        node.state().nodeState = NodeState.LEADER;
        node.state().epoch = 7;

        RaftNode.CommitResult result = node.propose("command".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        assertEquals(1, result.logIndex());
        assertEquals(1, node.state().commitIndex);
        assertEquals(1, node.state().lastApplied);
        assertArrayEquals("command".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                node.state().log.getEntry(1).orElseThrow().command());
        node.stop();
    }

    private RaftNode awaitLeader(List<RaftNode> nodes) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            List<RaftNode> leaders = nodes.stream().filter(RaftNode::isLeader).toList();
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            Thread.sleep(25);
        }
        fail("Cluster did not elect a single leader");
        return null;
    }
}
