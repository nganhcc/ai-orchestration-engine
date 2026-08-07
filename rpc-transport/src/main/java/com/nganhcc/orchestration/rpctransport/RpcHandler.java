package com.nganhcc.orchestration.rpctransport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Trạm cuối pipeline chiều nhận. Chạy trên server (worker) hoặc client (orchestrator)
 * tuỳ instance được truyền RpcHandler nào vào RpcChannelInitializer.
 * Ở Phase 2 chỉ cần xử lý HEARTBEAT -> ACK để chạy demo echo; ASSIGN_STEP thật
 * để dành Phase 5.
 */
public class RpcHandler extends SimpleChannelInboundHandler<FrameMessage> {

    // Shared across handler instances on a worker process: highest epoch seen from any leader.
    // Use AtomicLong to update safely across Netty IO threads.
    private static final java.util.concurrent.atomic.AtomicLong highestEpochSeen = new java.util.concurrent.atomic.AtomicLong(0L);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FrameMessage msg) {
        switch (msg.messageType()) {
            case FrameMessage.HEARTBEAT -> handleHeartbeat(ctx, msg);
            case FrameMessage.ASSIGN_STEP -> handleAssignStep(ctx, msg);
            case FrameMessage.ACK -> handleAck(ctx, msg);
            case FrameMessage.STALE_LEADER_REJECT -> handleStaleLeaderReject(ctx, msg);
            default -> throw new IllegalArgumentException(
                    "Chưa xử lý messageType=" + msg.messageType());
        }
    }

    private void handleHeartbeat(ChannelHandlerContext ctx, FrameMessage msg) {
        // Fencing: accept only if msg.epoch >= highestEpochSeen.
        long incoming = msg.epoch();
        if (!acceptEpoch(incoming)) {
            // reply STALE_LEADER_REJECT with current highestEpochSeen
            FrameMessage reject = new FrameMessage(FrameMessage.STALE_LEADER_REJECT,
                    msg.requestId(), highestEpochSeen.get(), new byte[0]);
            ctx.writeAndFlush(reject);
            return;
        }

        // Echo back ACK for heartbeat. Accepting the epoch already set highestEpochSeen.
        FrameMessage ack = new FrameMessage(FrameMessage.ACK, msg.requestId(), incoming, new byte[0]);
        ctx.writeAndFlush(ack);
    }

    private void handleAssignStep(ChannelHandlerContext ctx, FrameMessage msg) {
        // Fencing: reject if epoch stale before any state mutation / payload parsing
        long incoming = msg.epoch();
        if (!acceptEpoch(incoming)) {
            FrameMessage reject = new FrameMessage(FrameMessage.STALE_LEADER_REJECT,
                    msg.requestId(), highestEpochSeen.get(), new byte[0]);
            ctx.writeAndFlush(reject);
            return;
        }

        // Đến đây mới cần biết cấu trúc lồng bên trong payload -> parse tiếp
        AssignStepPayload payload = FrameCodec.decodeAssignStepPayload(msg.payload());
        // Phase 2: mock xử lý (thay cho LlmClient) và trả STEP_RESULT về orchestrator
        System.out.printf("Nhận ASSIGN_STEP: jobId=%s stepId=%s traceId=%s epoch=%d%n",
            payload.jobId(), payload.stepId(), payload.traceId(), incoming);

        // Mock processing: produce a simple JSON result. In real Phase 5 this calls LlmClient.
        String resultJson = "{\"status\":\"ok\",\"stepId\":\"" + payload.stepId() + "\"}";
        byte[] resultBytes = resultJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        // Build STEP_RESULT payload and send back on same channel
        StepResultPayload resultPayload = new StepResultPayload(payload.traceId(), payload.jobId(), payload.stepId(), resultBytes);
        byte[] payloadBytes = FrameCodec.encodeStepResultPayload(resultPayload);

        FrameMessage stepResult = new FrameMessage(FrameMessage.STEP_RESULT, msg.requestId(), incoming, payloadBytes);
        ctx.writeAndFlush(stepResult);
    }

    private void handleAck(ChannelHandlerContext ctx, FrameMessage msg) {
        System.out.printf("Nhận ACK cho requestId=%d, epoch=%d%n", msg.requestId(), msg.epoch());
    }

    private void handleStaleLeaderReject(ChannelHandlerContext ctx, FrameMessage msg) {
        System.err.printf("Nhận STALE_LEADER_REJECT cho requestId=%d, leader_epoch_moi=%d. Ngừng xử lý!%n",
                msg.requestId(), msg.epoch());
        // Có thể bổ sung logic update highestEpochSeen nếu orchestrator trả về epoch mới
        long newEpoch = msg.epoch();
        acceptEpoch(newEpoch);
    }

    private boolean acceptEpoch(long incomingEpoch) {
        // If incoming < highest seen -> reject. If >=, update highestEpochSeen to incoming (max).
        long prev;
        do {
            prev = highestEpochSeen.get();
            if (incomingEpoch < prev) return false;
            if (incomingEpoch == prev) return true; // equal is acceptable
        } while (!highestEpochSeen.compareAndSet(prev, incomingEpoch));
        return true;
    }

    // TEST-HOOK: allow tests to reset or set the highest epoch observed.
    // Package-private on purpose (tests in same package can call it).
    static void setHighestEpochForTests(long epoch) {
        highestEpochSeen.set(epoch);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Bắt buộc override — nếu không, exception ném trong channelRead0 (ví dụ decode
        // AssignStepPayload lỗi) sẽ chỉ log ra console mặc định của Netty rồi im lặng đóng
        // kết nối, rất khó debug khi test end-to-end.
        System.err.println("Lỗi xử lý frame: " + cause.getMessage());
        ctx.close();
    }
}