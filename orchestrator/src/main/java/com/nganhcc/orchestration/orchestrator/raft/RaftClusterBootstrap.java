package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.LogEntry;
import com.nganhcc.orchestration.raftcore.RaftLogStore;
import com.nganhcc.orchestration.raftcore.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import jakarta.annotation.PreDestroy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

//khi khởi động spring sẽ gọi run method -> config raft từ env -> tạo raftnode,transport,server -> start raftnode,server -> log ra raft node đã start
//Tạo ra 1 object raftnode, 1 object transport, 1 object server 
@Component
public final class RaftClusterBootstrap implements ApplicationRunner, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftClusterBootstrap.class);

    private final JdbcTemplate jdbcTemplate;

    private volatile RaftNode raftNode;
    private volatile RaftNettyTransport transport;
    private volatile RaftNettyServer server;  //lắng nghe các request từ các node khác (Vote, AppendEntries, LeadreRequest) rồi chuyển frame tới raftNode xử lí

    @Autowired
    public RaftClusterBootstrap(@Autowired(required = false) JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public RaftNode raftNode() {
        return raftNode;
    }

    @Override
    public void run(ApplicationArguments args) {
        RaftClusterConfig.fromEnvironment(System.getenv()).ifPresentOrElse(config -> {
            try {
                transport = new RaftNettyTransport(config.peers());
                RaftLogStore store = jdbcTemplate != null ? new JdbcRaftLogStore(jdbcTemplate) : RaftLogStore.NO_OP;
                raftNode = new RaftNode(
                        config.selfId(),
                        Set.copyOf(config.peers().keySet()),
                        transport,
                        new LoggingRaftEventListener(),
                        store);
                restoreFromStore(config.selfId(), store);
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

    /**
     * Khôi phục log + snapshot từ store (PostgreSQL) trước khi node bắt đầu election/heartbeat.
     * Nếu không có store (NO_OP) thì không làm gì.
     */
    private void restoreFromStore(String nodeId, RaftLogStore store) {
        if (store == RaftLogStore.NO_OP) {
            return;
        }
        RaftLogStore.SnapshotMeta snapshot = store.loadSnapshot(nodeId);
        if (snapshot != null) {
            raftNode.state().log.restoreSnapshotOffset(snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
            raftNode.state().lastSnapshotIndex = snapshot.lastIncludedIndex();
            raftNode.state().lastSnapshotTerm = snapshot.lastIncludedTerm();
            if (raftNode.state().commitIndex < snapshot.lastIncludedIndex()) {
                raftNode.state().commitIndex = snapshot.lastIncludedIndex();
            }
            if (raftNode.state().lastApplied < snapshot.lastIncludedIndex()) {
                raftNode.state().lastApplied = snapshot.lastIncludedIndex();
            }
            log.info("raft.restore.snapshot nodeId={} lastIncludedIndex={} lastIncludedTerm={}",
                    nodeId, snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm());
        }
        List<LogEntry> entries = store.load(nodeId);
        raftNode.state().log.restoreEntries(entries);
        if (!entries.isEmpty()) {
            log.info("raft.restore.log nodeId={} restoredEntries={} lastIndex={} lastTerm={}",
                    nodeId, entries.size(), raftNode.state().log.lastIndex(), raftNode.state().log.lastTerm());
        }

        RaftLogStore.HardState hardState = store.loadHardState(nodeId);
        if (hardState != null) {
            raftNode.state().currentTerm = Math.max(0, hardState.currentTerm());
            raftNode.state().votedFor = hardState.votedFor();
            long lastIndex = raftNode.state().log.lastIndex();
            long snapshotIndex = raftNode.state().log.getSnapshotOffset();
            raftNode.state().commitIndex = clampIndex(hardState.commitIndex(), snapshotIndex, lastIndex);
            raftNode.state().lastApplied = clampIndex(hardState.lastApplied(), snapshotIndex, lastIndex);
            log.info("raft.restore.meta nodeId={} term={} votedFor={} commitIndex={} lastApplied={}",
                    nodeId,
                    raftNode.state().currentTerm,
                    raftNode.state().votedFor,
                    raftNode.state().commitIndex,
                    raftNode.state().lastApplied);
        }
    }

    private long clampIndex(long value, long minimum, long maximum) {
        return Math.max(minimum, Math.min(value, maximum));
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
