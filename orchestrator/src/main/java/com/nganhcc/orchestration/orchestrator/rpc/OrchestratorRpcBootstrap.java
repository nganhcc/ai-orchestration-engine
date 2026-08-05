package com.nganhcc.orchestration.orchestrator.rpc;

import com.nganhcc.orchestration.orchestrator.service.StepService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

@Component
final class OrchestratorRpcBootstrap implements ApplicationRunner, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorRpcBootstrap.class);

    private final StepService stepService;
    private WorkerRpcServer server;

    OrchestratorRpcBootstrap(StepService stepService) {
        this.stepService = stepService;
    }

    @Override
    public void run(ApplicationArguments args) {
        OrchestratorRpcConfig.fromEnvironment(System.getenv()).ifPresentOrElse(config -> {
            try {
                var handler = new com.nganhcc.orchestration.orchestrator.rpc.StepResultRpcHandler(stepService);
                server = new WorkerRpcServer(config.bindHost(), config.port(), handler);
                server.start();
                log.info("orchestrator.rpc.started host={} port={}", config.bindHost(), config.port());
            } catch (RuntimeException e) {
                close();
                throw e;
            }
        }, () -> log.info("ORCHESTRATOR_RPC_PORT not set; skipping RPC server startup"));
    }

    @PreDestroy
    public void close() {
        if (server != null) server.close();
    }
}
