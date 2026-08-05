package com.nganhcc.orchestration.worker;

import com.nganhcc.orchestration.rpctransport.RpcChannelInitializer;
import com.nganhcc.orchestration.rpctransport.RpcHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class WorkerRpcClientRunner implements CommandLineRunner {

    @Value("${ORCHESTRATOR_HOST:orchestrator-a}")
    private String orchestratorHost;

    @Value("${ORCHESTRATOR_PORT:9000}")
    private int orchestratorPort;

    @Override
    public void run(String... args) {
        Thread t = new Thread(this::connectLoop, "worker-rpc-client");
        t.setDaemon(false);
        t.start();
    }

    private void connectLoop() {
        while (true) {
            EventLoopGroup group = new NioEventLoopGroup();
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new RpcChannelInitializer(new RpcHandler()));

                ChannelFuture f = b.connect(orchestratorHost, orchestratorPort).sync();
                Channel ch = f.channel();
                System.out.println("Worker connected to orchestrator " + orchestratorHost + ":" + orchestratorPort);
                ch.closeFuture().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println("Worker RPC client connection failed: " + e.getMessage());
            } finally {
                group.shutdownGracefully().syncUninterruptibly();
            }

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
