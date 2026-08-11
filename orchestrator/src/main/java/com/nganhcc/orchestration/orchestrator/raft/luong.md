Node A (client)                                 Node B (server)
-----------------------------------------------------------------
1. POJO request   (RequestVoteRequest)
2. encode → FrameMessage  (RaftWireCodec)
3. Netty pipeline (RpcEncoder, LengthFieldPrepender) → TCP
4. receive → Netty pipeline (LengthFieldBasedFrameDecoder,
   RpcDecoder) → FrameMessage
5. RaftServerHandler.channelRead0()   <-- FrameMessage
6. decode → POJO request  (RaftWireCodec)
7. RaftNode.onReceiveRequestVote(request) → POJO response
8. encode response → FrameMessage (RaftWireCodec, same requestId)
9. ctx.writeAndFlush(response FrameMessage) → Netty pipeline (RpcEncoder)
10. TCP back to Node A
11. Netty pipeline (RpcDecoder) → FrameMessage (response)
12. RaftClientResponseHandler → match requestId → complete CompletableFuture
13. Caller (RaftNettyTransport.send) receives CompletableFuture<FrameMessage>
14. decode response FrameMessage → POJO (RaftWireCodec)
15. RaftNode continues processing (e.g., becomes leader)
