package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
final class RaftClusterBootstrap implements ApplicationRunner, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftClusterBootstrap.class);

    private volatile RaftNode raftNode;
    private volatile RaftNettyTransport transport;
    private volatile RaftNettyServer server;

    @Override
    public void run(ApplicationArguments args) {
        RaftClusterConfig.fromEnvironment(System.getenv()).ifPresentOrElse(config -> {
            try {
                transport = new RaftNettyTransport(config.peers());
                raftNode = new RaftNode(
                        config.selfId(),
                        Set.copyOf(config.peers().keySet()),
                        transport,
                        new LoggingRaftEventListener());
                server = new RaftNettyServer(config.bindHost(), config.port(), raftNode);
                server.start();
                raftNode.start();
                log.info(
                        "raft.bootstrap.started nodeId={} port={} peers={}",
                        config.selfId(),
                        config.port(),
                        config.peers().keySet());
            } catch (RuntimeException e) {
                close();
                throw e;
            }
        }, () -> log.info("RAFT env chưa được cấu hình, bỏ qua bootstrap cluster"));
    }

    @PreDestroy
    public void close() {
        if (raftNode != null) {
            raftNode.stop();
        }
        if (server != null) {
            server.close();
        }
        if (transport != null) {
            transport.close();
        }
    }
}
