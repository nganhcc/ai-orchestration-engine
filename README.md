# ai-orchestration-engine

Distributed AI orchestration engine prototype.

## Phase 3 demo

Build the orchestrator jar first:

```bash
./gradlew :orchestrator:bootJar
```

Start the local demo stack:

```bash
docker compose up -d --build postgres redis orchestrator-a orchestrator-b orchestrator-c 
```

Watch Raft leader election in the logs:

```bash
docker compose logs -f orchestrator-a orchestrator-b orchestrator-c
```

When one node logs `raft.event=leader.elected`, kill that service and watch the cluster re-elect:

```bash
docker compose kill orchestrator-a
```

The orchestrator services boot in Raft-only mode when these env vars are present:

```text
SPRING_MAIN_WEB_APPLICATION_TYPE=none
RAFT_NODE_ID=orchestrator-a|orchestrator-b|orchestrator-c
RAFT_PORT=7000
RAFT_BIND_HOST=0.0.0.0
RAFT_PEERS=orchestrator-b=orchestrator-b:7000,orchestrator-c=orchestrator-c:7000
```
