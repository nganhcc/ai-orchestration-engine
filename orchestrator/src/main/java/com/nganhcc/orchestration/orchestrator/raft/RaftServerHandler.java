package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.RaftMessages.AppendEntriesRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.AppendEntriesResponse;
import com.nganhcc.orchestration.raftcore.RaftMessages.RequestVoteRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.RequestVoteResponse;
import com.nganhcc.orchestration.raftcore.RaftMessages.InstallSnapshotRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.InstallSnapshotResponse;
import com.nganhcc.orchestration.raftcore.RaftNode;
import com.nganhcc.orchestration.rpctransport.FrameMessage;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

//RaftServerHandler là handler Netty được gắn vào pipeline của máy chủ (RaftNettyServer).
//Nó nhận các frame (FrameMessage) mà các peer gửi tới qua TCP, giải mã chúng thành các 
//request Raft (RequestVote, AppendEntries) và gửi chúng tới đối tượng core RaftNode. 
//Sau khi RaftNode xử lý, handler mã hoá đáp trả (Response) và gửi lại cho peer qua Netty.
@ChannelHandler.Sharable
final class RaftServerHandler extends SimpleChannelInboundHandler<FrameMessage> {

    private final RaftNode node;

    RaftServerHandler(RaftNode node) {
        this.node = node;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FrameMessage msg) {
        switch (msg.messageType()) {
            case FrameMessage.RAFT_REQUEST_VOTE -> handleRequestVote(ctx, msg);
            case FrameMessage.RAFT_APPEND_ENTRIES -> handleAppendEntries(ctx, msg);
            case FrameMessage.RAFT_INSTALL_SNAPSHOT -> handleInstallSnapshot(ctx, msg);
            default -> throw new IllegalArgumentException("Unsupported Raft messageType=" + msg.messageType());
        }
    }

    private void handleRequestVote(ChannelHandlerContext ctx, FrameMessage msg) {
        RequestVoteRequest request = RaftWireCodec.decodeRequestVoteRequest(msg);
        RequestVoteResponse response = node.onReceiveRequestVote(request);
        ctx.writeAndFlush(RaftWireCodec.encodeRequestVoteResponse(msg.requestId(), response));
    }

    private void handleAppendEntries(ChannelHandlerContext ctx, FrameMessage msg) {
        AppendEntriesRequest request = RaftWireCodec.decodeAppendEntriesRequest(msg);
        AppendEntriesResponse response = node.onReceiveAppendEntries(request);
        ctx.writeAndFlush(RaftWireCodec.encodeAppendEntriesResponse(msg.requestId(), response));
    }

    private void handleInstallSnapshot(ChannelHandlerContext ctx, FrameMessage msg) {
        InstallSnapshotRequest request = RaftWireCodec.decodeInstallSnapshotRequest(msg);
        InstallSnapshotResponse response = node.onReceiveInstallSnapshot(request);
        ctx.writeAndFlush(RaftWireCodec.encodeInstallSnapshotResponse(msg.requestId(), response));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
