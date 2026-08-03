package com.nganhcc.orchestration.rpctransport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class EchoServer {

    private final int port;

    public EchoServer(int port) {
        this.port = port;
    }

    public void start() throws InterruptedException {
        // bossGroup: chỉ lo accept connection mới, không xử lý dữ liệu.
        // workerGroup: xử lý I/O thật (đọc/ghi) cho từng connection đã accept.
        // Tách 2 group vì accept và xử lý dữ liệu là 2 việc khác bản chất — accept
        // cần nhanh, không nên bị block bởi 1 connection đang xử lý dữ liệu nặng.
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new RpcChannelInitializer(new RpcHandler()));
            // childHandler: áp dụng cho mỗi Channel CON được accept (mỗi client kết nối tới)
            // khác với .handler(...) — cái đó áp dụng cho chính ServerChannel (Channel cha, chỉ lo accept)

            ChannelFuture f = b.bind(port).sync(); // block tới khi bind xong
            System.out.println("EchoServer đang lắng nghe ở port " + port);

            f.channel().closeFuture().sync(); // block tới khi server bị đóng
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9000;
        new EchoServer(port).start();
    }
}