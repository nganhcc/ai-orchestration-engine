package com.nganhcc.orchestration.rpctransport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class EchoClient {

    private final String host;
    private final int port;

    public EchoClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void run() throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        // Client dùng riêng RpcHandler để log ACK nhận về, khác RpcHandler phía server
        ClientAckHandler ackHandler = new ClientAckHandler();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new RpcChannelInitializer(ackHandler));

            ChannelFuture f = b.connect(host, port).sync();
            Channel channel = f.channel();
            System.out.println("Đã kết nối tới " + host + ":" + port);

            for (int i = 1; i <= 5; i++) {
                long requestId = i;
                FrameMessage heartbeat = new FrameMessage(
                        FrameMessage.HEARTBEAT, requestId, 1L, new byte[0]);

                long sentAt = System.nanoTime();
                ackHandler.recordSentTime(requestId, sentAt);

                channel.writeAndFlush(heartbeat);
                Thread.sleep(200); // giãn cách để log dễ đọc, không phải yêu cầu kỹ thuật
            }

            Thread.sleep(1000); // chờ ACK cuối cùng về trước khi đóng
            channel.close().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9000;
        new EchoClient(host, port).run();
    }
}