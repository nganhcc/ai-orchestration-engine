package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.Set;

//khi khởi động spring sẽ gọi run method -> config raft từ env -> tạo raftnode,transport,server -> start raftnode,server -> log ra raft node đã start
//Tạo ra 1 object raftnode, 1 object transport, 1 object server 
@Component
public final class RaftClusterBootstrap implements ApplicationRunner, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftClusterBootstrap.class);

    private volatile RaftNode raftNode;
    private volatile RaftNettyTransport transport;
    private volatile RaftNettyServer server;  //lắng nghe các request từ các node khác (Vote, AppendEntries, LeadreRequest) rồi chuyển frame tới raftNode xử lí

    public RaftNode raftNode() {
        return raftNode;
    }

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
