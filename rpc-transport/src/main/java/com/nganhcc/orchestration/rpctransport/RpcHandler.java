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

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FrameMessage msg) {
        switch (msg.messageType()) {
            case FrameMessage.HEARTBEAT -> handleHeartbeat(ctx, msg);
            case FrameMessage.ASSIGN_STEP -> handleAssignStep(ctx, msg);
            default -> throw new IllegalArgumentException(
                    "Chưa xử lý messageType=" + msg.messageType());
        }
    }

    private void handleHeartbeat(ChannelHandlerContext ctx, FrameMessage msg) {
        // Echo lại đúng requestId để bên gửi match được request/response,
        // epoch giữ nguyên vì Phase 2 chưa check fencing (để dành Phase 4).
        FrameMessage ack = new FrameMessage(
                FrameMessage.ACK, msg.requestId(), msg.epoch(), new byte[0]);
        ctx.writeAndFlush(ack); // chảy ngược lên NettyFrameEncoder -> socket
    }

    private void handleAssignStep(ChannelHandlerContext ctx, FrameMessage msg) {
        // Đến đây mới cần biết cấu trúc lồng bên trong payload -> parse tiếp
        AssignStepPayload payload = FrameCodec.decodeAssignStepPayload(msg.payload());
        // Phase 2: chỉ log ra để verify pipeline chạy đúng, chưa gọi LlmClient thật (Phase 5)
        System.out.printf("Nhận ASSIGN_STEP: jobId=%s stepId=%s traceId=%s%n",
                payload.jobId(), payload.stepId(), payload.traceId());
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