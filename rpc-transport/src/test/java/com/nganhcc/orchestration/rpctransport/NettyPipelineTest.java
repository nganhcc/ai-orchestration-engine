package com.nganhcc.orchestration.rpctransport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NettyPipelineTest {

    // Dựng đúng pipeline giống RpcChannelInitializer, nhưng không cần Channel thật
    private EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4),
                new NettyFrameDecoder(),
                new NettyFrameEncoder()
        );
    }

    @Test
    void frameDenDuMotLan_decodeDungRaFrameMessage() {
        EmbeddedChannel channel = newChannel();
        FrameMessage original = new FrameMessage(FrameMessage.HEARTBEAT, 1001L, 7L, new byte[0]);
        byte[] wire = FrameCodec.encodeFrame(original);

        channel.writeInbound(Unpooled.copiedBuffer(wire)); // đẩy trọn 21 byte 1 lần

        FrameMessage decoded = channel.readInbound();
        assertEquals(original, decoded);
    }

    @Test
    void frameBiChiaLamHaiGoiTcp_vanDecodeDungKhiDuByte() {
        EmbeddedChannel channel = newChannel();
        FrameMessage original = new FrameMessage(FrameMessage.HEARTBEAT, 1001L, 7L, new byte[0]);
        byte[] wire = FrameCodec.encodeFrame(original); // 21 byte

        byte[] firstHalf = java.util.Arrays.copyOfRange(wire, 0, 10);
        byte[] secondHalf = java.util.Arrays.copyOfRange(wire, 10, wire.length);

        channel.writeInbound(Unpooled.copiedBuffer(firstHalf));
        // Mới có 10/21 byte -> LengthFieldBasedFrameDecoder phải TỰ CHỜ, chưa đẩy gì xuống
        assertNull(channel.readInbound());

        channel.writeInbound(Unpooled.copiedBuffer(secondHalf));
        // Đủ byte -> giờ mới ra FrameMessage
        FrameMessage decoded = channel.readInbound();
        assertEquals(original, decoded);
    }

    @Test
    void haiFrameDinhLienNhauTrongMotGoiTcp_tachDungTungFrame() {
        EmbeddedChannel channel = newChannel();
        FrameMessage first = new FrameMessage(FrameMessage.HEARTBEAT, 1L, 1L, new byte[0]);
        FrameMessage second = new FrameMessage(FrameMessage.ACK, 2L, 1L, new byte[0]);

        ByteBuf combined = Unpooled.wrappedBuffer(
                FrameCodec.encodeFrame(first), FrameCodec.encodeFrame(second));
        channel.writeInbound(combined);

        assertEquals(first, (FrameMessage) channel.readInbound());
        assertEquals(second, (FrameMessage) channel.readInbound());
        assertNull(channel.readInbound()); // không còn frame nào khác
    }

    @Test
    void encode_roiDecodeNguocLai_chayTronVenQuaCaHaiEncoder() {
        EmbeddedChannel channel = newChannel();
        FrameMessage original = new FrameMessage(
                FrameMessage.ASSIGN_STEP, 99L, 3L, "payload-test".getBytes());

        channel.writeOutbound(original);           // qua NettyFrameEncoder -> ByteBuf
        ByteBuf encoded = channel.readOutbound();

        channel.writeInbound(encoded);              // đẩy ngược lại qua decoder
        FrameMessage decoded = channel.readInbound();

        assertEquals(original, decoded);
    }
}