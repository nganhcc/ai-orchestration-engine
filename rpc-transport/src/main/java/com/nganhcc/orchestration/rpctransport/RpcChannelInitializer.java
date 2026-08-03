package com.nganhcc.orchestration.rpctransport;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class RpcChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final ChannelHandler businessHandler; // đổi từ RpcHandler -> ChannelHandler (interface chung)

    public RpcChannelInitializer(ChannelHandler businessHandler) {
        this.businessHandler = businessHandler;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
                .addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4))
                .addLast(new NettyFrameDecoder())
                .addLast(new NettyFrameEncoder())
                .addLast(businessHandler);
    }
}