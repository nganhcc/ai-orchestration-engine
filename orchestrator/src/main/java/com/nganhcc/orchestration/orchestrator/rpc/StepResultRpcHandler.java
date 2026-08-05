package com.nganhcc.orchestration.orchestrator.rpc;

import com.nganhcc.orchestration.orchestrator.service.StepService;
import com.nganhcc.orchestration.rpctransport.FrameCodec;
import com.nganhcc.orchestration.rpctransport.FrameMessage;
import com.nganhcc.orchestration.rpctransport.StepResultPayload;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Handler running inside the orchestrator that receives `STEP_RESULT` frames from workers.
 * On receiving a STEP_RESULT it calls `StepService.markStepDone(...)` and replies with
 * `ACK` when applied or `STALE_LEADER_REJECT` when rejected.
 */
public final class StepResultRpcHandler extends SimpleChannelInboundHandler<FrameMessage> {

    private final StepService stepService;

    public StepResultRpcHandler(StepService stepService) {
        this.stepService = stepService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FrameMessage msg) {
        if (msg.messageType() != FrameMessage.STEP_RESULT) {
            // Ignore other message types in this handler
            return;
        }

        StepResultPayload payload = FrameCodec.decodeStepResultPayload(msg.payload());
        UUID stepId = UUID.fromString(payload.stepId());
        long epoch = msg.epoch();
        String resultJson = new String(payload.result(), StandardCharsets.UTF_8);

        boolean applied = stepService.markStepDone(stepId, epoch, resultJson);
        if (applied) {
            FrameMessage ack = new FrameMessage(FrameMessage.ACK, msg.requestId(), epoch, new byte[0]);
            ctx.writeAndFlush(ack);
        } else {
            FrameMessage reject = new FrameMessage(FrameMessage.STALE_LEADER_REJECT, msg.requestId(), epoch, new byte[0]);
            ctx.writeAndFlush(reject);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
