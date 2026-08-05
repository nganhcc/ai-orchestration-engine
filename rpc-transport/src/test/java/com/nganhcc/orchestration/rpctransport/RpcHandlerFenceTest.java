package com.nganhcc.orchestration.rpctransport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RpcHandlerFenceTest {

    @Test
    void heartbeat_updatesEpoch_then_staleAssignRejected() {
        // Reset static epoch state for deterministic test
        RpcHandler.setHighestEpochForTests(0L);

        EmbeddedChannel ch = new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4),
                new NettyFrameDecoder(),
                new NettyFrameEncoder(),
                new RpcHandler()
        );

        // Send heartbeat with epoch=10 -> expect ACK with epoch=10
        FrameMessage hb = new FrameMessage(FrameMessage.HEARTBEAT, 101L, 10L, new byte[0]);
        ch.writeInbound(Unpooled.wrappedBuffer(FrameCodec.encodeFrame(hb)));

        Object out1 = ch.readOutbound();
        assertNotNull(out1);
        assertTrue(out1 instanceof ByteBuf);
        byte[] respBytes1 = new byte[((ByteBuf) out1).readableBytes()];
        ((ByteBuf) out1).readBytes(respBytes1);
        FrameMessage resp1 = FrameCodec.decodeFrame(respBytes1);
        assertEquals(FrameMessage.ACK, resp1.messageType());
        assertEquals(10L, resp1.epoch());

        // Now send ASSIGN_STEP with lower epoch=9 -> expect STALE_LEADER_REJECT with epoch=10
        AssignStepPayload payload = new AssignStepPayload(java.util.UUID.randomUUID(), "j", "s", new byte[0]);
        byte[] payloadBytes = FrameCodec.encodeAssignStepPayload(payload);
        FrameMessage assign = new FrameMessage(FrameMessage.ASSIGN_STEP, 102L, 9L, payloadBytes);
        ch.writeInbound(Unpooled.wrappedBuffer(FrameCodec.encodeFrame(assign)));

        Object out2 = ch.readOutbound();
        assertNotNull(out2);
        assertTrue(out2 instanceof ByteBuf);
        byte[] respBytes2 = new byte[((ByteBuf) out2).readableBytes()];
        ((ByteBuf) out2).readBytes(respBytes2);
        FrameMessage resp2 = FrameCodec.decodeFrame(respBytes2);
        assertEquals(FrameMessage.STALE_LEADER_REJECT, resp2.messageType());
        assertEquals(10L, resp2.epoch());

        ch.finishAndReleaseAll();
    }

    @Test
    void assignWithHigherEpochAccepted_noRejectSent() {
        RpcHandler.setHighestEpochForTests(0L);

        EmbeddedChannel ch = new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4),
                new NettyFrameDecoder(),
                new NettyFrameEncoder(),
                new RpcHandler()
        );

        AssignStepPayload payload = new AssignStepPayload(java.util.UUID.randomUUID(), "j2", "s2", new byte[0]);
        byte[] payloadBytes = FrameCodec.encodeAssignStepPayload(payload);
        FrameMessage assign = new FrameMessage(FrameMessage.ASSIGN_STEP, 201L, 20L, payloadBytes);
        ch.writeInbound(Unpooled.wrappedBuffer(FrameCodec.encodeFrame(assign)));

        // RpcHandler in this phase mocks processing and sends STEP_RESULT back
        Object out = ch.readOutbound();
        assertNotNull(out);
        assertTrue(out instanceof io.netty.buffer.ByteBuf);
        byte[] respBytes = new byte[((io.netty.buffer.ByteBuf) out).readableBytes()];
        ((io.netty.buffer.ByteBuf) out).readBytes(respBytes);
        FrameMessage resp = FrameCodec.decodeFrame(respBytes);
        assertEquals(FrameMessage.STEP_RESULT, resp.messageType());
        assertEquals(20L, resp.epoch());
        // verify payload decodes to StepResultPayload with matching stepId
        StepResultPayload srp = FrameCodec.decodeStepResultPayload(resp.payload());
        assertEquals("s2", srp.stepId());

        ch.finishAndReleaseAll();
    }
}
