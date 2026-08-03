package com.nganhcc.orchestration.rpctransport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientAckHandler extends SimpleChannelInboundHandler<FrameMessage> {

    // requestId -> thời điểm gửi (nanoTime), dùng để tính round-trip khi ACK về
    private final Map<Long, Long> sentTimestamps = new ConcurrentHashMap<>();

    public void recordSentTime(long requestId, long nanoTime) {
        sentTimestamps.put(requestId, nanoTime);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FrameMessage msg) {
        if (msg.messageType() == FrameMessage.ACK) {
            Long sentAt = sentTimestamps.remove(msg.requestId());
            if (sentAt != null) {
                long rttNanos = System.nanoTime() - sentAt;
                System.out.printf("ACK requestId=%d, round-trip = %.2f ms%n",
                        msg.requestId(), rttNanos / 1_000_000.0);
            } else {
                System.out.println("Nhận ACK cho requestId không rõ: " + msg.requestId());
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("Lỗi phía client: " + cause.getMessage());
        ctx.close();
    }
}