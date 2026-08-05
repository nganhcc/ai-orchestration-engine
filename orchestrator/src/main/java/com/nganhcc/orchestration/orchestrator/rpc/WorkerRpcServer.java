package com.nganhcc.orchestration.orchestrator.rpc;

import com.nganhcc.orchestration.rpctransport.RpcChannelInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.ChannelHandler;

import java.net.InetSocketAddress;

public final class WorkerRpcServer implements AutoCloseable {

    private final String bindHost;
    private final int port;
    private final ChannelHandler businessHandler;
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private volatile Channel serverChannel;

    public WorkerRpcServer(String bindHost, int port, ChannelHandler businessHandler) {
        this.bindHost = bindHost;
        this.port = port;
        this.businessHandler = businessHandler;
    }

    public synchronized void start() {
        if (serverChannel != null) return;
        try {
                // If bindHost is the IPv4 wildcard, bind explicitly to an IPv4 wildcard
                // address to avoid creating an IPv6-only listener on some systems.
                ServerBootstrap bootstrap = new ServerBootstrap()
                        .group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childOption(ChannelOption.SO_KEEPALIVE, true)
                        .childHandler(new RpcChannelInitializer(businessHandler));

                ChannelFuture future;
                if ("0.0.0.0".equals(bindHost)) {
                    try {
                        var inet = java.net.InetAddress.getByAddress(new byte[]{0, 0, 0, 0});
                        future = bootstrap.bind(new InetSocketAddress(inet, port)).syncUninterruptibly();
                    } catch (java.net.UnknownHostException e) {
                        future = bootstrap.bind(port).syncUninterruptibly();
                    }
                } else if (bindHost == null || bindHost.isEmpty()) {
                    future = bootstrap.bind(port).syncUninterruptibly();
                } else {
                    future = bootstrap.bind(new InetSocketAddress(bindHost, port)).syncUninterruptibly();
                }
            if (!future.isSuccess()) {
                throw new IllegalStateException("Failed binding Worker RPC server", future.cause());
            }
            serverChannel = future.channel();
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    public int port() {
        Channel c = serverChannel;
        if (c == null) return port;
        return ((InetSocketAddress) c.localAddress()).getPort();
    }

    @Override
    public synchronized void close() {
        Channel c = serverChannel;
        serverChannel = null;
        if (c != null) c.close().syncUninterruptibly();
        bossGroup.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }
}
