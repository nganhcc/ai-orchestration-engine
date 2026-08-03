package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.RaftMessages.AppendEntriesRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.AppendEntriesResponse;
import com.nganhcc.orchestration.raftcore.RaftMessages.RequestVoteRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.RequestVoteResponse;
import com.nganhcc.orchestration.raftcore.RaftTransport;
import com.nganhcc.orchestration.rpctransport.FrameMessage;
import com.nganhcc.orchestration.rpctransport.RpcChannelInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public final class RaftNettyTransport implements RaftTransport, AutoCloseable {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(100);

    private final Map<String, InetSocketAddress> peerAddresses;
    private final EventLoopGroup clientGroup;
    private final Bootstrap bootstrap;
    private final RaftClientResponseHandler responseHandler;
    private final ConcurrentHashMap<String, Channel> channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, CompletableFuture<FrameMessage>> inflight = new ConcurrentHashMap<>();
    private final AtomicLong requestIdSequence = new AtomicLong(1L);
    private final Duration timeout;

    public RaftNettyTransport(Map<String, InetSocketAddress> peerAddresses) {
        this(peerAddresses, DEFAULT_TIMEOUT);
    }

    public RaftNettyTransport(Map<String, InetSocketAddress> peerAddresses, Duration timeout) {
        this.peerAddresses = Map.copyOf(Objects.requireNonNull(peerAddresses, "peerAddresses"));
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.clientGroup = new NioEventLoopGroup(1);
        this.responseHandler = new RaftClientResponseHandler(inflight);
        this.bootstrap = new Bootstrap()
                .group(clientGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) timeout.toMillis())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new RpcChannelInitializer(responseHandler));
    }

    @Override
    public RequestVoteResponse sendRequestVote(String targetNodeId, RequestVoteRequest req) {
        FrameMessage response = send(targetNodeId, RaftWireCodec.encodeRequestVoteRequest(nextRequestId(), req));
        return RaftWireCodec.decodeRequestVoteResponse(response);
    }

    @Override
    public AppendEntriesResponse sendAppendEntries(String targetNodeId, AppendEntriesRequest req) {
        FrameMessage response = send(targetNodeId, RaftWireCodec.encodeAppendEntriesRequest(nextRequestId(), req));
        return RaftWireCodec.decodeAppendEntriesResponse(response);
    }

    private long nextRequestId() {
        return requestIdSequence.getAndIncrement();
    }

    private FrameMessage send(String targetNodeId, FrameMessage request) {
        Channel channel = getOrConnect(targetNodeId);
        CompletableFuture<FrameMessage> future = new CompletableFuture<>();
        CompletableFuture<FrameMessage> previous = inflight.putIfAbsent(request.requestId(), future);
        if (previous != null) {
            throw new IllegalStateException("Duplicate requestId " + request.requestId());
        }

        channel.writeAndFlush(request).addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                CompletableFuture<FrameMessage> pending = inflight.remove(request.requestId());
                if (pending != null) {
                    pending.completeExceptionally(writeFuture.cause());
                }
            }
        });

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            inflight.remove(request.requestId());
            throw new IllegalStateException(
                    "Timeout waiting Raft response from " + targetNodeId + " after " + timeout.toMillis() + "ms", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            inflight.remove(request.requestId());
            throw new IllegalStateException("Interrupted while waiting Raft response from " + targetNodeId, e);
        } catch (Exception e) {
            inflight.remove(request.requestId());
            throw new IllegalStateException("Failed sending Raft request to " + targetNodeId, e);
        }
    }

    private Channel getOrConnect(String targetNodeId) {
        Channel channel = channels.get(targetNodeId);
        if (channel != null && channel.isActive()) {
            return channel;
        }
        synchronized (channels) {
            channel = channels.get(targetNodeId);
            if (channel != null && channel.isActive()) {
                return channel;
            }
            InetSocketAddress address = peerAddresses.get(targetNodeId);
            if (address == null) {
                throw new IllegalArgumentException("Unknown Raft peer: " + targetNodeId);
            }
            var connectFuture = bootstrap.connect(address).syncUninterruptibly();
            if (!connectFuture.isSuccess()) {
                throw new IllegalStateException("Failed connecting to Raft peer " + targetNodeId, connectFuture.cause());
            }
            Channel connected = connectFuture.channel();
            connected.closeFuture().addListener(future -> channels.remove(targetNodeId, connected));
            channels.put(targetNodeId, connected);
            return connected;
        }
    }

    @Override
    public void close() {
        channels.values().forEach(channel -> {
            if (channel != null) {
                channel.close().syncUninterruptibly();
            }
        });
        clientGroup.shutdownGracefully().syncUninterruptibly();
    }

    @ChannelHandler.Sharable
    private static final class RaftClientResponseHandler extends io.netty.channel.SimpleChannelInboundHandler<FrameMessage> {

        private final ConcurrentHashMap<Long, CompletableFuture<FrameMessage>> inflight;

        private RaftClientResponseHandler(ConcurrentHashMap<Long, CompletableFuture<FrameMessage>> inflight) {
            this.inflight = inflight;
        }

        @Override
        protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx, FrameMessage msg) {
            CompletableFuture<FrameMessage> future = inflight.remove(msg.requestId());
            if (future != null) {
                future.complete(msg);
            }
        }

        @Override
        public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
