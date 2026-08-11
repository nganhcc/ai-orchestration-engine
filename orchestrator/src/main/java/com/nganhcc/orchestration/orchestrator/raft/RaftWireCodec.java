package com.nganhcc.orchestration.orchestrator.raft;

import com.nganhcc.orchestration.raftcore.LogEntry;
import com.nganhcc.orchestration.raftcore.RaftMessages.AppendEntriesRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.AppendEntriesResponse;
import com.nganhcc.orchestration.raftcore.RaftMessages.RequestVoteRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.RequestVoteResponse;
import com.nganhcc.orchestration.raftcore.RaftMessages.InstallSnapshotRequest;
import com.nganhcc.orchestration.raftcore.RaftMessages.InstallSnapshotResponse;
import com.nganhcc.orchestration.rpctransport.FrameMessage;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

//FrameMessage: Dinh nghia cau truc frame truyen giua cac node: goi tin nhi phan 
//RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest, AppendEntriesResponse: gom 4 truong: messageType, requestId, epoch, payload(byte[])
//RequestVoteRequest: payload gom: term, candidateId, lastLogIndex, lastLogTerm
//RequestVoteResponse: payload gom: term, voteGranted
//AppendEntriesRequest: payload gom: term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, entries
//AppendEntriesResponse: payload gom: term, success, matchIndex(ptional)
public final class RaftWireCodec {

    private RaftWireCodec() {}

    public static FrameMessage encodeRequestVoteRequest(long requestId, RequestVoteRequest request) {
        return new FrameMessage(
                FrameMessage.RAFT_REQUEST_VOTE,
                requestId,
                0L,
                encodeRequestVoteRequestPayload(request));
    }

    public static RequestVoteRequest decodeRequestVoteRequest(FrameMessage frame) {
        requireType(frame, FrameMessage.RAFT_REQUEST_VOTE);
        return decodeRequestVoteRequestPayload(frame.payload());
    }

    public static FrameMessage encodeRequestVoteResponse(long requestId, RequestVoteResponse response) {
        return new FrameMessage(
                FrameMessage.RAFT_REQUEST_VOTE_RESPONSE,
                requestId,
                0L,
                encodeRequestVoteResponsePayload(response));
    }

    public static RequestVoteResponse decodeRequestVoteResponse(FrameMessage frame) {
        requireType(frame, FrameMessage.RAFT_REQUEST_VOTE_RESPONSE);
        return decodeRequestVoteResponsePayload(frame.payload());
    }

    public static FrameMessage encodeAppendEntriesRequest(long requestId, AppendEntriesRequest request) {
        return new FrameMessage(
                FrameMessage.RAFT_APPEND_ENTRIES,
                requestId,
                0L,
                encodeAppendEntriesRequestPayload(request));
    }

    public static AppendEntriesRequest decodeAppendEntriesRequest(FrameMessage frame) {
        requireType(frame, FrameMessage.RAFT_APPEND_ENTRIES);
        return decodeAppendEntriesRequestPayload(frame.payload());
    }

    public static FrameMessage encodeAppendEntriesResponse(long requestId, AppendEntriesResponse response) {
        return new FrameMessage(
                FrameMessage.RAFT_APPEND_ENTRIES_RESPONSE,
                requestId,
                0L,
                encodeAppendEntriesResponsePayload(response));
    }

    public static AppendEntriesResponse decodeAppendEntriesResponse(FrameMessage frame) {
        requireType(frame, FrameMessage.RAFT_APPEND_ENTRIES_RESPONSE);
        return decodeAppendEntriesResponsePayload(frame.payload());
    }

    public static FrameMessage encodeInstallSnapshotRequest(long requestId, InstallSnapshotRequest request) {
        return new FrameMessage(
                FrameMessage.RAFT_INSTALL_SNAPSHOT,
                requestId,
                0L,
                encodeInstallSnapshotRequestPayload(request));
    }

    public static InstallSnapshotRequest decodeInstallSnapshotRequest(FrameMessage frame) {
        requireType(frame, FrameMessage.RAFT_INSTALL_SNAPSHOT);
        return decodeInstallSnapshotRequestPayload(frame.payload());
    }

    public static FrameMessage encodeInstallSnapshotResponse(long requestId, InstallSnapshotResponse response) {
        return new FrameMessage(
                FrameMessage.RAFT_INSTALL_SNAPSHOT_RESPONSE,
                requestId,
                0L,
                encodeInstallSnapshotResponsePayload(response));
    }

    public static InstallSnapshotResponse decodeInstallSnapshotResponse(FrameMessage frame) {
        requireType(frame, FrameMessage.RAFT_INSTALL_SNAPSHOT_RESPONSE);
        return decodeInstallSnapshotResponsePayload(frame.payload());
    }

    public static byte[] encodeRequestVoteRequestPayload(RequestVoteRequest request) {
        byte[] candidateIdBytes = utf8Bytes(request.candidateId(), "candidateId");
        ByteBuffer buffer = ByteBuffer.allocate(8 + 2 + candidateIdBytes.length + 8 + 8);
        buffer.putLong(request.term());
        putBytesWithShortLength(buffer, candidateIdBytes, "candidateId");
        buffer.putLong(request.lastLogIndex());
        buffer.putLong(request.lastLogTerm());
        return buffer.array();
    }

    public static RequestVoteRequest decodeRequestVoteRequestPayload(byte[] bytes) {
        ByteBuffer buffer = wrap(bytes, "RequestVoteRequest");
        try {
            long term = buffer.getLong();
            String candidateId = readStringWithShortLength(buffer, "candidateId");
            long lastLogIndex = buffer.getLong();
            long lastLogTerm = buffer.getLong();
            ensureConsumed(buffer, "RequestVoteRequest");
            return new RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
        } catch (BufferUnderflowException e) {
            throw malformed("RequestVoteRequest", "payload quá ngắn", e);
        }
    }

    public static byte[] encodeRequestVoteResponsePayload(RequestVoteResponse response) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 1);
        buffer.putLong(response.term());
        buffer.put((byte) (response.voteGranted() ? 1 : 0));
        return buffer.array();
    }

    public static RequestVoteResponse decodeRequestVoteResponsePayload(byte[] bytes) {
        ByteBuffer buffer = wrap(bytes, "RequestVoteResponse");
        try {
            long term = buffer.getLong();
            boolean voteGranted = readBoolean(buffer, "voteGranted");
            ensureConsumed(buffer, "RequestVoteResponse");
            return new RequestVoteResponse(term, voteGranted);
        } catch (BufferUnderflowException e) {
            throw malformed("RequestVoteResponse", "payload quá ngắn", e);
        }
    }

    public static byte[] encodeAppendEntriesRequestPayload(AppendEntriesRequest request) {
        List<LogEntry> entries = request.entries() == null ? List.of() : request.entries();
        int size = 8 + 2 + utf8Bytes(request.leaderId(), "leaderId").length + 8 + 8 + 8 + 4;
        for (LogEntry entry : entries) {
            size += 8 + 8 + 4 + entry.command().length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(request.term());
        putBytesWithShortLength(buffer, utf8Bytes(request.leaderId(), "leaderId"), "leaderId");
        buffer.putLong(request.prevLogIndex());
        buffer.putLong(request.prevLogTerm());
        buffer.putLong(request.leaderCommit());
        buffer.putInt(entries.size());
        for (LogEntry entry : entries) {
            byte[] command = entry.command();
            buffer.putLong(entry.term());
            buffer.putLong(entry.index());
            buffer.putInt(command.length);
            buffer.put(command);
        }
        return buffer.array();
    }

    public static AppendEntriesRequest decodeAppendEntriesRequestPayload(byte[] bytes) {
        ByteBuffer buffer = wrap(bytes, "AppendEntriesRequest");
        try {
            long term = buffer.getLong();
            String leaderId = readStringWithShortLength(buffer, "leaderId");
            long prevLogIndex = buffer.getLong();
            long prevLogTerm = buffer.getLong();
            long leaderCommit = buffer.getLong();
            int entryCount = buffer.getInt();
            if (entryCount < 0) {
                throw new IllegalArgumentException("AppendEntriesRequest entryCount không hợp lệ: " + entryCount);
            }
            List<LogEntry> entries = new ArrayList<>(entryCount);
            for (int i = 0; i < entryCount; i++) {
                long entryTerm = buffer.getLong();
                long entryIndex = buffer.getLong();
                int commandLength = buffer.getInt();
                if (commandLength < 0 || commandLength > buffer.remaining()) {
                    throw new IllegalArgumentException(
                            "AppendEntriesRequest commandLength không hợp lệ ở entry " + i + ": " + commandLength);
                }
                byte[] command = new byte[commandLength];
                buffer.get(command);
                entries.add(new LogEntry(entryTerm, entryIndex, command));
            }
            ensureConsumed(buffer, "AppendEntriesRequest");
            return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, List.copyOf(entries), leaderCommit);
        } catch (BufferUnderflowException e) {
            throw malformed("AppendEntriesRequest", "payload quá ngắn", e);
        }
    }

    public static byte[] encodeAppendEntriesResponsePayload(AppendEntriesResponse response) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 1 + 8);
        buffer.putLong(response.term());
        buffer.put((byte) (response.success() ? 1 : 0));
        buffer.putLong(response.matchIndex());
        return buffer.array();
    }

    public static AppendEntriesResponse decodeAppendEntriesResponsePayload(byte[] bytes) {
        ByteBuffer buffer = wrap(bytes, "AppendEntriesResponse");
        try {
            long term = buffer.getLong();
            boolean success = readBoolean(buffer, "success");
            long matchIndex = buffer.getLong();
            ensureConsumed(buffer, "AppendEntriesResponse");
            return new AppendEntriesResponse(term, success, matchIndex);
        } catch (BufferUnderflowException e) {
            throw malformed("AppendEntriesResponse", "payload quá ngắn", e);
        }
    }

    public static byte[] encodeInstallSnapshotRequestPayload(InstallSnapshotRequest request) {
        byte[] leaderIdBytes = utf8Bytes(request.leaderId(), "leaderId");
        byte[] data = request.data() == null ? new byte[0] : request.data();
        ByteBuffer buffer = ByteBuffer.allocate(8 + 2 + leaderIdBytes.length + 8 + 8 + 4 + data.length);
        buffer.putLong(request.term());
        putBytesWithShortLength(buffer, leaderIdBytes, "leaderId");
        buffer.putLong(request.lastIncludedIndex());
        buffer.putLong(request.lastIncludedTerm());
        buffer.putInt(data.length);
        buffer.put(data);
        return buffer.array();
    }

    public static InstallSnapshotRequest decodeInstallSnapshotRequestPayload(byte[] bytes) {
        ByteBuffer buffer = wrap(bytes, "InstallSnapshotRequest");
        try {
            long term = buffer.getLong();
            String leaderId = readStringWithShortLength(buffer, "leaderId");
            long lastIncludedIndex = buffer.getLong();
            long lastIncludedTerm = buffer.getLong();
            int dataLen = buffer.getInt();
            if (dataLen < 0 || dataLen > buffer.remaining()) {
                throw new IllegalArgumentException("InstallSnapshotRequest dataLen không hợp lệ: " + dataLen);
            }
            byte[] data = new byte[dataLen];
            buffer.get(data);
            ensureConsumed(buffer, "InstallSnapshotRequest");
            return new InstallSnapshotRequest(term, leaderId, lastIncludedIndex, lastIncludedTerm, data);
        } catch (BufferUnderflowException e) {
            throw malformed("InstallSnapshotRequest", "payload quá ngắn", e);
        }
    }

    public static byte[] encodeInstallSnapshotResponsePayload(InstallSnapshotResponse response) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 1);
        buffer.putLong(response.term());
        buffer.put((byte) (response.success() ? 1 : 0));
        return buffer.array();
    }

    public static InstallSnapshotResponse decodeInstallSnapshotResponsePayload(byte[] bytes) {
        ByteBuffer buffer = wrap(bytes, "InstallSnapshotResponse");
        try {
            long term = buffer.getLong();
            boolean success = readBoolean(buffer, "success");
            ensureConsumed(buffer, "InstallSnapshotResponse");
            return new InstallSnapshotResponse(term, success);
        } catch (BufferUnderflowException e) {
            throw malformed("InstallSnapshotResponse", "payload quá ngắn", e);
        }
    }

    private static void requireType(FrameMessage frame, byte expectedType) {
        if (frame.messageType() != expectedType) {
            throw new IllegalArgumentException(
                    "messageType không khớp, expected=" + expectedType + ", actual=" + frame.messageType());
        }
    }

    private static ByteBuffer wrap(byte[] bytes, String typeName) {
        if (bytes == null) {
            throw new IllegalArgumentException(typeName + " payload không được null");
        }
        return ByteBuffer.wrap(bytes);
    }

    private static byte[] utf8Bytes(String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " không được null");
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void putBytesWithShortLength(ByteBuffer buffer, byte[] bytes, String fieldName) {
        if (bytes.length > 0xFFFF) {
            throw new IllegalArgumentException(fieldName + " vượt quá 65535 byte: " + bytes.length);
        }
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    private static String readStringWithShortLength(ByteBuffer buffer, String fieldName) {
        int length = Short.toUnsignedInt(buffer.getShort());
        if (length > buffer.remaining()) {
            throw new IllegalArgumentException(fieldName + " length vượt quá số byte còn lại: " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static boolean readBoolean(ByteBuffer buffer, String fieldName) {
        byte value = buffer.get();
        if (value != 0 && value != 1) {
            throw new IllegalArgumentException(fieldName + " không hợp lệ: " + value);
        }
        return value == 1;
    }

    private static void ensureConsumed(ByteBuffer buffer, String typeName) {
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException(typeName + " còn thừa " + buffer.remaining() + " byte");
        }
    }

    private static IllegalArgumentException malformed(String typeName, String message, Throwable cause) {
        return new IllegalArgumentException(typeName + " malformed: " + message, cause);
    }
}
