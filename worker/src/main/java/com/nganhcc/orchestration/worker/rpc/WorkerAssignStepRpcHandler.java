package com.nganhcc.orchestration.worker.rpc;

import com.nganhcc.orchestration.rpctransport.AssignStepPayload;
import com.nganhcc.orchestration.rpctransport.FrameCodec;
import com.nganhcc.orchestration.rpctransport.FrameMessage;
import com.nganhcc.orchestration.rpctransport.StepResultPayload;
import com.nganhcc.orchestration.worker.service.WorkerStepService;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerAssignStepRpcHandler extends SimpleChannelInboundHandler<FrameMessage> {

    private static final Logger log = LoggerFactory.getLogger(WorkerAssignStepRpcHandler.class);

    private final WorkerStepService workerStepService;
    private final AtomicLong highestEpochSeen = new AtomicLong(0);

    public WorkerAssignStepRpcHandler(WorkerStepService workerStepService) {
        this.workerStepService = workerStepService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FrameMessage msg) {
        if (msg.messageType() != FrameMessage.ASSIGN_STEP) {
            // Forward or ignore non-ASSIGN_STEP messages
            ctx.fireChannelRead(msg);
            return;
        }

        long epoch = msg.epoch();
        long currentHighest = highestEpochSeen.get();
        if (epoch < currentHighest) {
            log.warn("WorkerAssignStepRpcHandler: Rejecting ASSIGN_STEP due to stale leader epoch {} < {}", epoch, currentHighest);
            FrameMessage reject = new FrameMessage(FrameMessage.STALE_LEADER_REJECT, msg.requestId(), epoch, new byte[0]);
            ctx.writeAndFlush(reject);
            return;
        }
        highestEpochSeen.accumulateAndGet(epoch, Math::max);

        AssignStepPayload assignPayload = FrameCodec.decodeAssignStepPayload(msg.payload());
        UUID stepId = UUID.fromString(assignPayload.stepId());
        String jobId = assignPayload.jobId();
        String documentId = new String(assignPayload.payload(), StandardCharsets.UTF_8);

        log.info("WorkerAssignStepRpcHandler: Received ASSIGN_STEP stepId={} jobId={} doc={}", stepId, jobId, documentId);

        try {
            String resultJson = workerStepService.processStep(stepId, jobId, documentId);

            StepResultPayload resultPayload = new StepResultPayload(
                    assignPayload.traceId(),
                    jobId,
                    stepId.toString(),
                    resultJson.getBytes(StandardCharsets.UTF_8)
            );

            byte[] encodedResult = FrameCodec.encodeStepResultPayload(resultPayload);
            FrameMessage stepResultMsg = new FrameMessage(FrameMessage.STEP_RESULT, msg.requestId(), epoch, encodedResult);

            ctx.writeAndFlush(stepResultMsg);
        } catch (com.nganhcc.orchestration.worker.service.CircuitBreaker.CircuitOpenException e) {
            log.warn("WorkerAssignStepRpcHandler: Circuit breaker is OPEN. Dropping stepId={} so it will be reaped. Msg: {}", stepId, e.getMessage());
            // Do not reply
        } catch (Exception e) {
            log.error("WorkerAssignStepRpcHandler: Failed to process stepId={} due to LLM call error: {}. Dropping so it will be reaped.", stepId, e.getMessage());
            // Do not reply
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in WorkerAssignStepRpcHandler: {}", cause.getMessage(), cause);
        ctx.close();
    }
}
