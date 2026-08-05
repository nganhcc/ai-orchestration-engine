package com.nganhcc.orchestration.orchestrator.rpc;

import java.util.Optional;

public final class OrchestratorRpcConfig {
    private final String bindHost;
    private final int port;

    private OrchestratorRpcConfig(String bindHost, int port) {
        this.bindHost = bindHost;
        this.port = port;
    }

    public String bindHost() { return bindHost; }
    public int port() { return port; }

    public static Optional<OrchestratorRpcConfig> fromEnvironment(java.util.Map<String,String> env) {
        String portValue = env.get("ORCHESTRATOR_RPC_PORT");
        if (portValue == null) return Optional.empty();
        String bindHost = env.getOrDefault("ORCHESTRATOR_RPC_BIND_HOST", "0.0.0.0");
        int port = Integer.parseInt(portValue);
        return Optional.of(new OrchestratorRpcConfig(bindHost, port));
    }
}
