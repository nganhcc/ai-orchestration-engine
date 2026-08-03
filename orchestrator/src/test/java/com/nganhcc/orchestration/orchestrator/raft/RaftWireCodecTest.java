package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.LogEntry;
import com.nganhcc.orchestration.raftcore.RaftMessages.AppendEntriesRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.AppendEntriesResponse;
import com.nganhcc.orchestration.raftcore.RaftMessages.RequestVoteRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.RequestVoteResponse;
import com.nganhcc.orchestration.rpctransport.FrameMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RaftWireCodecTest {

    @Test
    void requestVote_roundTrip_keepsUtf8NodeId() {
        RequestVoteRequest request = new RequestVoteRequest(7L, "nút-1", 12L, 5L);
        FrameMessage frame = RaftWireCodec.encodeRequestVoteRequest(99L, request);

        assertEquals(FrameMessage.RAFT_REQUEST_VOTE, frame.messageType());
        assertEquals(0L, frame.epoch());
        assertEquals(request, RaftWireCodec.decodeRequestVoteRequest(frame));
    }

    @Test
    void requestVoteResponse_roundTrip() {
        RequestVoteResponse response = new RequestVoteResponse(9L, true);
        FrameMessage frame = RaftWireCodec.encodeRequestVoteResponse(100L, response);

        assertEquals(FrameMessage.RAFT_REQUEST_VOTE_RESPONSE, frame.messageType());
        assertEquals(response, RaftWireCodec.decodeRequestVoteResponse(frame));
    }

    @Test
    void appendEntries_roundTrip_withEmptyEntriesAndOneEntry() {
        AppendEntriesRequest emptyHeartbeat = new AppendEntriesRequest(
                3L,
                "leader-1",
                10L,
                8L,
                List.of(),
                10L);
        FrameMessage heartbeatFrame = RaftWireCodec.encodeAppendEntriesRequest(1L, emptyHeartbeat);
        assertEquals(emptyHeartbeat, RaftWireCodec.decodeAppendEntriesRequest(heartbeatFrame));

        LogEntry entry = new LogEntry(4L, 11L, "payload".getBytes(StandardCharsets.UTF_8));
        AppendEntriesRequest withEntry = new AppendEntriesRequest(
                4L,
                "leader-2",
                10L,
                8L,
                List.of(entry),
                11L);
        FrameMessage entryFrame = RaftWireCodec.encodeAppendEntriesRequest(2L, withEntry);
        assertEquals(withEntry, RaftWireCodec.decodeAppendEntriesRequest(entryFrame));
    }

    @Test
    void appendEntriesResponse_roundTrip() {
        AppendEntriesResponse response = new AppendEntriesResponse(11L, false, 23L);
        FrameMessage frame = RaftWireCodec.encodeAppendEntriesResponse(3L, response);

        assertEquals(FrameMessage.RAFT_APPEND_ENTRIES_RESPONSE, frame.messageType());
        assertEquals(response, RaftWireCodec.decodeAppendEntriesResponse(frame));
    }

    @Test
    void malformedPayload_throwsClearException() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> RaftWireCodec.decodeRequestVoteRequestPayload(new byte[4]));

        assertTrue(ex.getMessage().contains("RequestVoteRequest"));
    }

    @Test
    void requestVote_payloadPreservesShortStringLengths() {
        String nodeId = UUID.randomUUID().toString();
        RequestVoteRequest request = new RequestVoteRequest(1L, nodeId, 2L, 3L);
        byte[] payload = RaftWireCodec.encodeRequestVoteRequestPayload(request);
        assertEquals(request, RaftWireCodec.decodeRequestVoteRequestPayload(payload));
    }
}
