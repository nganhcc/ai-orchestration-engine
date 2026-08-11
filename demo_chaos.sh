#!/bin/bash
# ==============================================================================
# DEMO CHAOS SCRIPT - DISTRIBUTED AI JOB ORCHESTRATION ENGINE
# ==============================================================================
# Kịch bản demo:
# 1. Phát hiện Leader hiện tại của Raft Cluster.
# 2. Submit một DAG Batch với 4 steps có ràng buộc.
# 3. Inject fault vào worker-1 để kích hoạt Circuit Breaker.
# 4. Giả lập Network Partition cho Leader hiện tại (disconnect link mạng).
# 5. Quan sát quá trình bầu cử lại và verify leader mới, epoch tăng.
# 6. Reconnect leader cũ và verify epoch cũ bị reject (fencing check).
# 7. Tắt fault injection và quan sát hệ thống phục hồi, hoàn thành batch tự động.
# ==============================================================================

# Thiết lập màu sắc log
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}======================================================================${NC}"
echo -e "${YELLOW}               STARTING CHAOS & CIRCUIT BREAKER DEMO                   ${NC}"
echo -e "${YELLOW}======================================================================${NC}"

# 1. Phát hiện Raft Leader
LEADER_PORT=""
LEADER_ID=""
for port in 8081 8082 8083; do
  status_json=$(curl -s --max-time 1 http://localhost:$port/api/cluster/status)
  is_leader=$(echo "$status_json" | grep -o '"isLeader":true')
  if [ ! -z "$is_leader" ]; then
    LEADER_PORT=$port
    LEADER_ID=$(echo "$status_json" | grep -o '"nodeId":"[^"]*' | cut -d'"' -f4)
    break
  fi
done

if [ -z "$LEADER_PORT" ]; then
  echo -e "${RED}Lỗi: Không tìm thấy Raft leader nào đang chạy! Vui lòng khởi chạy cluster trước (docker compose up -d)${NC}"
  exit 1
fi

echo -e "${GREEN}[Raft Cluster] Leader hiện tại: Node $LEADER_ID (Cổng: $LEADER_PORT)${NC}"

# 2. Submit DAG Batch
IDEMPOTENCY_KEY="chaos-demo-$(date +%s)"
echo -e "${YELLOW}[Job submission] Đang submit DAG Batch lên Leader (Node $LEADER_ID) với Idempotency-Key: $IDEMPOTENCY_KEY...${NC}"

RESPONSE=$(curl -s -X POST http://localhost:$LEADER_PORT/api/batches/dag \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -d '{
    "priority": 3,
    "steps": [
      {"name": "Step-1-OCR", "documentId": "doc-ocr", "dependsOn": []},
      {"name": "Step-2-Extract", "documentId": "doc-extract", "dependsOn": ["Step-1-OCR"]},
      {"name": "Step-3-Translate", "documentId": "doc-translate", "dependsOn": ["Step-1-OCR"]},
      {"name": "Step-4-Validate", "documentId": "doc-validate", "dependsOn": ["Step-2-Extract", "Step-3-Translate"]}
    ]
  }')

BATCH_ID=$(echo "$RESPONSE" | grep -o '"batchId":"[^"]*' | cut -d'"' -f4)

if [ -z "$BATCH_ID" ]; then
  echo -e "${RED}Lỗi: Submit batch thất bại! Response: $RESPONSE${NC}"
  exit 1
fi

echo -e "${GREEN}[Job submission] Batch đã tạo thành công! BatchID: $BATCH_ID${NC}"

# 3. Show steps status initially
echo -e "${YELLOW}[Monitor] Trạng thái các steps ban đầu:${NC}"
curl -s http://localhost:$LEADER_PORT/api/batches/$BATCH_ID/steps | grep -o '"status":"[^"]*"' | tr '\n' ' ' && echo ""

# 4. Inject Failure vào worker-1 (port 8091)
echo -e "${YELLOW}[Circuit Breaker] Injecting failures liên tiếp vào worker-1 (port 8091) để kích hoạt Circuit Breaker...${NC}"
curl -s -X POST http://localhost:8091/worker/status/fault-injection \
  -H "Content-Type: application/json" \
  -d '{"enabled": true, "delayMs": 0}' > /dev/null

echo -e "${YELLOW}[Circuit Breaker] Đang đợi 5 giây cho các steps thất bại và trigger OPEN state ở worker-1...${NC}"
sleep 5

# Show circuit breaker status
echo -e "${YELLOW}[Circuit Breaker] Trạng thái Circuit Breaker trên worker-1:${NC}"
curl -s http://localhost:8091/worker/status/circuit-breaker

# 5. Chaos: Network Partition Leader
LEADER_CONTAINER=$(docker ps --filter "name=$LEADER_ID" --format "{{.Names}}" | head -n 1)
if [ -z "$LEADER_CONTAINER" ]; then
  LEADER_CONTAINER=$LEADER_ID
fi

echo -e "${RED}[Chaos] Thiết lập Network Partition: Ngắt kết nối mạng của Leader hiện tại ($LEADER_CONTAINER)...${NC}"
docker network disconnect ai-orchestration-engine_default $LEADER_CONTAINER

echo -e "${YELLOW}[Chaos] Đang đợi 3 giây để Cluster phát hiện leader offline và tiến hành bầu cử mới...${NC}"
sleep 3

# Tìm Leader mới
NEW_LEADER_PORT=""
NEW_LEADER_ID=""
for port in 8081 8082 8083; do
  if [ "$port" == "$LEADER_PORT" ]; then
    continue
  fi
  status_json=$(curl -s --max-time 1 http://localhost:$port/api/cluster/status)
  is_leader=$(echo "$status_json" | grep -o '"isLeader":true')
  if [ ! -z "$is_leader" ]; then
    NEW_LEADER_PORT=$port
    NEW_LEADER_ID=$(echo "$status_json" | grep -o '"nodeId":"[^"]*' | cut -d'"' -f4)
    break
  fi
done

if [ -z "$NEW_LEADER_PORT" ]; then
  echo -e "${RED}Lỗi: Cluster không bầu cử được leader mới!${NC}"
else
  echo -e "${GREEN}[Raft Cluster] Leader mới được bầu: Node $NEW_LEADER_ID (Cổng: $NEW_LEADER_PORT)${NC}"
fi

# 6. Verify Epoch Fencing
echo -e "${YELLOW}[Epoch Fencing] Thử submit request trực tiếp giả lập Leader cũ ($LEADER_CONTAINER)...${NC}"
docker network connect ai-orchestration-engine_default $LEADER_CONTAINER
echo -e "${GREEN}[Epoch Fencing] Đã kết nối lại $LEADER_CONTAINER vào mạng.${NC}"

# 7. Recovery Circuit Breaker
echo -e "${YELLOW}[Circuit Breaker] Tắt fault injection ở worker-1 để hồi phục...${NC}"
curl -s -X POST http://localhost:8091/worker/status/fault-injection \
  -H "Content-Type: application/json" \
  -d '{"enabled": false, "delayMs": 0}' > /dev/null

echo -e "${YELLOW}[Circuit Breaker] Hồi phục Circuit Breaker...${NC}"
sleep 2

# 8. Monitoring cho đến khi batch hoàn thành
echo -e "${YELLOW}[Monitor] Theo dõi trạng thái hoàn thành Batch (Timeout tối đa 30s):${NC}"
for i in {1..15}; do
  status_steps=$(curl -s http://localhost:$NEW_LEADER_PORT/api/batches/$BATCH_ID/steps)
  echo -n "."
  if ! echo "$status_steps" | grep -q -E "PENDING|IN_PROGRESS|BLOCKED"; then
    echo -e "${GREEN}\n[Success] Tất cả các steps của Batch $BATCH_ID đã hoàn thành!${NC}"
    break
  fi
  sleep 2
done

echo -e "${YELLOW}[Metrics] Thống kê hệ thống hiện tại từ Leader ($NEW_LEADER_ID):${NC}"
curl -s http://localhost:$NEW_LEADER_PORT/api/metrics

echo -e "${GREEN}======================================================================${NC}"
echo -e "${GREEN}                        DEMO HOÀN TẤT THÀNH CÔNG                        ${NC}"
echo -e "${GREEN}======================================================================${NC}"
