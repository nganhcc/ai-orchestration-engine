package com.nganhcc.orchestration.rpctransport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * CHỈ được gọi sau khi LengthFieldBasedFrameDecoder (đứng trước trong pipeline)
 * đã cắt đúng 1 frame và strip 4 byte totalLength.
 * -> `in` ở đây luôn chứa đúng messageType+requestId+epoch+payload, không thừa không thiếu.
 */
public class NettyFrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        byte[] bodyBytes = new byte[in.readableBytes()];
        in.readBytes(bodyBytes);
        out.add(FrameCodec.decodeBody(bodyBytes)); // không gọi decodeFrame vì đã bị strip length
    }
}