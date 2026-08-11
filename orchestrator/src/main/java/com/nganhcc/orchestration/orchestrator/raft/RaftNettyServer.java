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

// class này để start server netty lắng nghe các request từ raft node khác
public final class RaftNettyServer implements AutoCloseable {

    private final String bindHost;//localhost
    private final int port;//8081
    private final RaftNode node;//tham chieu den raft node
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(1);//nhom thread boss chiu trach nhiem accept cac ket noi den
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(1);//nhom thread worker chiu trach nhiem xu ly du lieu cua cac node da duoc chap nhan tu bossgroup
    private volatile Channel serverChannel;// kenh netty daij dien cho socket dang lang nghe

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
                    //rpcChannelInitializer là pipeline de xu ly cac ket noi den, no chua raftserverhandler de xu ly request tu raft node khac

                    //RpcChannelInitializer tạo pipeline:
                    //1.LengthFieldBasedFrameDecoder (4‑byte length prefix) → đảm bảo nhận đúng khung.
                    //2.RpcDecoder → chuyển ByteBuf thành RaftMessage.
                    //3.RpcEncoder (cho outbound).
                    //4.RaftServerHandler (đánh callback tới RaftNode)
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
