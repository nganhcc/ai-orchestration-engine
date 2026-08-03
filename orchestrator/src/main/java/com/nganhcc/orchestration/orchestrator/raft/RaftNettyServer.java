package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.RaftNode;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import com.nganhcc.orchestration.rpctransport.RpcChannelInitializer;

import java.net.InetSocketAddress;
import java.util.Objects;

public final class RaftNettyServer implements AutoCloseable {

    private final String bindHost;
    private final int port;
    private final RaftNode node;
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private volatile Channel serverChannel;

    public RaftNettyServer(String bindHost, int port, RaftNode node) {
        this.bindHost = Objects.requireNonNull(bindHost, "bindHost");
        this.port = port;
        this.node = Objects.requireNonNull(node, "node");
    }

    public synchronized void start() {
        if (serverChannel != null) {
            return;
        }
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new RpcChannelInitializer(new RaftServerHandler(node)))
                    .bind(new InetSocketAddress(bindHost, port))
                    .syncUninterruptibly();
            if (!future.isSuccess()) {
                throw new IllegalStateException("Failed binding Raft server on " + bindHost + ":" + port, future.cause());
            }
            serverChannel = future.channel();
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    public int port() {
        Channel channel = serverChannel;
        if (channel == null) {
            return port;
        }
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    @Override
    public synchronized void close() {
        Channel channel = serverChannel;
        serverChannel = null;
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        bossGroup.shutdownGracefully().syncUninterruptibly();
        workerGroup.shutdownGracefully().syncUninterruptibly();
    }
}
