package com.nganhcc.orchestration.orchestrator.raft;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

record RaftClusterConfig(
        String selfId,
        String bindHost,
        int port,
        Map<String, InetSocketAddress> peers
) {
    static Optional<RaftClusterConfig> fromEnvironment(Map<String, String> env) {
        Objects.requireNonNull(env, "env");

        String selfId = trimToNull(env.get("RAFT_NODE_ID"));
        String portValue = trimToNull(env.get("RAFT_PORT"));
        String peersValue = trimToNull(env.get("RAFT_PEERS"));
        if (selfId == null && portValue == null && peersValue == null) {
            return Optional.empty();
        }
        if (selfId == null || portValue == null || peersValue == null) {
            throw new IllegalArgumentException("Thiếu RAFT_NODE_ID / RAFT_PORT / RAFT_PEERS");
        }

        int port = Integer.parseInt(portValue);
        String bindHost = trimToNull(env.get("RAFT_BIND_HOST"));
        if (bindHost == null) {
            bindHost = "0.0.0.0";
        }

        Map<String, InetSocketAddress> peers = parsePeers(peersValue);
        return Optional.of(new RaftClusterConfig(selfId, bindHost, port, peers));
    }

    private static Map<String, InetSocketAddress> parsePeers(String peersValue) {
        Map<String, InetSocketAddress> peers = new LinkedHashMap<>();
        for (String rawPair : peersValue.split(",")) {
            String pair = rawPair.trim();
            if (pair.isEmpty()) {
                continue;
            }
            String[] parts = pair.split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Peer không hợp lệ: " + pair);
            }
            String nodeId = trimToNull(parts[0]);
            String[] hostPort = parts[1].trim().split(":", 2);
            if (nodeId == null || hostPort.length != 2) {
                throw new IllegalArgumentException("Peer không hợp lệ: " + pair);
            }
            String host = trimToNull(hostPort[0]);
            String portText = trimToNull(hostPort[1]);
            if (host == null || portText == null) {
                throw new IllegalArgumentException("Peer không hợp lệ: " + pair);
            }
            peers.put(nodeId, new InetSocketAddress(host, Integer.parseInt(portText)));
        }
        return Map.copyOf(peers);
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
