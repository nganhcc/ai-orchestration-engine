package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.NodeState;
import com.nganhcc.orchestration.raftcore.RaftNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RaftNettyClusterIntegrationTest {

    @Test
    void threeNodeClusterElectsLeaderAndRecoversAfterLeaderCrash() throws Exception {
        int portA = freePort();
        int portB = freePort();
        int portC = freePort();

        Bundle a = null;
        Bundle b = null;
        Bundle c = null;
        try {
            a = startBundle(
                    "A",
                    portA,
                    Map.of("B", new InetSocketAddress("127.0.0.1", portB), "C", new InetSocketAddress("127.0.0.1", portC)));
            b = startBundle(
                    "B",
                    portB,
                    Map.of("A", new InetSocketAddress("127.0.0.1", portA), "C", new InetSocketAddress("127.0.0.1", portC)));
            c = startBundle(
                    "C",
                    portC,
                    Map.of("A", new InetSocketAddress("127.0.0.1", portA), "B", new InetSocketAddress("127.0.0.1", portB)));

            a.node().start();
            b.node().start();
            c.node().start();

            List<Bundle> cluster = List.of(a, b, c);
            Bundle leader = awaitSingleLeader(cluster, Duration.ofSeconds(8));
            long leaderTerm = leader.node().state().currentTerm;
            assertEquals(NodeState.LEADER, leader.node().state().nodeState);

            leader.close();

            List<Bundle> remaining = new ArrayList<>(cluster);
            remaining.remove(leader);
            Bundle newLeader = awaitSingleLeader(remaining, Duration.ofSeconds(8));
            assertEquals(NodeState.LEADER, newLeader.node().state().nodeState);
            assertTrue(newLeader.node().state().currentTerm > leaderTerm, "Term phải tăng sau khi bầu lại");
        } finally {
            closeQuietly(a);
            closeQuietly(b);
            closeQuietly(c);
        }
    }

    private Bundle startBundle(String nodeId, int port, Map<String, InetSocketAddress> peers) {
        RaftNettyTransport transport = new RaftNettyTransport(peers, Duration.ofMillis(500));
        RaftNode node = new RaftNode(nodeId, Set.copyOf(peers.keySet()), transport);
        RaftNettyServer server = new RaftNettyServer("127.0.0.1", port, node);
        server.start();
        return new Bundle(nodeId, node, server, transport);
    }

    private Bundle awaitSingleLeader(List<Bundle> cluster, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            List<Bundle> leaders = cluster.stream()
                    .filter(bundle -> bundle.node().state().nodeState == NodeState.LEADER)
                    .toList();
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            Thread.sleep(25);
        }
        fail("Không bầu được đúng 1 leader trong " + timeout);
        return null;
    }

    private void closeQuietly(Bundle bundle) {
        if (bundle == null) {
            return;
        }
        try {
            bundle.close();
        } catch (Exception ignored) {
        }
    }

    private int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Không lấy được port trống", e);
        }
    }

    private record Bundle(String nodeId, RaftNode node, RaftNettyServer server, RaftNettyTransport transport)
            implements AutoCloseable {
        @Override
        public void close() {
            node.stop();
            server.close();
            transport.close();
        }
    }
}
