package com.nganhcc.orchestration.worker.rpc;

import com.nganhcc.orchestration.rpctransport.RpcChannelInitializer;
import com.nganhcc.orchestration.rpctransport.RpcHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.net.InetSocketAddress;

@Component
public final class WorkerRpcBootstrap implements ApplicationRunner, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerRpcBootstrap.class);

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private volatile Channel serverChannel;

    @Override
    public void run(ApplicationArguments args) {
        String portStr = System.getenv().getOrDefault("WORKER_RPC_PORT", "9000");
        String bindHost = System.getenv().getOrDefault("WORKER_RPC_BIND_HOST", "0.0.0.0");
        int port = Integer.parseInt(portStr);

        try {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup(1);
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new RpcChannelInitializer(new RpcHandler()))
                    .bind(new InetSocketAddress(bindHost, port))
                    .syncUninterruptibly();

            if (!future.isSuccess()) throw new IllegalStateException("Failed binding Worker RPC server", future.cause());
            serverChannel = future.channel();
            log.info("worker.rpc.started host={} port={}", bindHost, port);
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    @PreDestroy
    @Override
    public void close() {
        Channel c = serverChannel;
        serverChannel = null;
        if (c != null) c.close().syncUninterruptibly();
        if (bossGroup != null) bossGroup.shutdownGracefully().syncUninterruptibly();
        if (workerGroup != null) workerGroup.shutdownGracefully().syncUninterruptibly();
    }
}
