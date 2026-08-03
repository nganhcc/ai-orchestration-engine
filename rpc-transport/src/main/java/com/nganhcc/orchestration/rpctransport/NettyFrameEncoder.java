package com.nganhcc.orchestration.rpctransport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class NettyFrameEncoder extends MessageToByteEncoder<FrameMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, FrameMessage msg, ByteBuf out) {
        // Dùng lại encodeFrame thuần (đã bao gồm 4 byte totalLength) để không viết trùng logic tính length.
        out.writeBytes(FrameCodec.encodeFrame(msg));
    }
}