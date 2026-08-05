package com.nganhcc.orchestration.rpctransport;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class FrameCodec {

    private FrameCodec() {}

    // ---------- Tầng ngoài: FrameMessage <-> byte[] ----------

    /**
     * Trả về TOÀN BỘ frame trên wire, bao gồm cả 4 byte totalLength ở đầu.
     * totalLength = messageType(1) + requestId(8) + epoch(8) + payload.length
     *             (không tính 4 byte của chính totalLength).
     */
    public static byte[] encodeFrame(FrameMessage msg) {
        int bodyLength = 1 + 8 + 8 + msg.payload().length;
        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLength); // ByteBuffer mặc định big-endian
        buf.putInt(bodyLength);
        buf.put(msg.messageType());
        buf.putLong(msg.requestId());
        buf.putLong(msg.epoch());
        buf.put(msg.payload());
        return buf.array();
    }

    /**
     * Input: bytes đã BAO GỒM 4 byte totalLength ở đầu (dùng khi test round-trip
     * độc lập với Netty). Khi gắn Netty ở bước sau, LengthFieldBasedFrameDecoder
     * sẽ tự cắt bỏ 4 byte này trước rồi mới gọi vào decodeBody() bên dưới.
     */
    public static FrameMessage decodeFrame(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int bodyLength = buf.getInt();
        if (bodyLength != bytes.length - 4) {
            throw new IllegalArgumentException(
                    "totalLength khai báo (" + bodyLength + ") không khớp số byte thực nhận ("
                            + (bytes.length - 4) + ")");
        }
        return decodeBody(buf);
    }

    /**
     * Dùng khi 4 byte length đã bị NettyFrameDecoder strip trước đó (initialBytesToStrip=4) —
     * input chỉ còn messageType+requestId+epoch+payload, không còn length field.
     */
    public static FrameMessage decodeBody(byte[] bodyBytes) {
        return decodeBody(ByteBuffer.wrap(bodyBytes));
    }

    private static FrameMessage decodeBody(ByteBuffer buf) {
        byte messageType = buf.get();
        long requestId = buf.getLong();
        long epoch = buf.getLong();
        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);
        return new FrameMessage(messageType, requestId, epoch, payload);
    }

    // ---------- Tầng trong: AssignStepPayload <-> byte[] ----------

    public static byte[] encodeAssignStepPayload(AssignStepPayload p) {
        byte[] jobIdBytes = p.jobId().getBytes(StandardCharsets.UTF_8);
        byte[] stepIdBytes = p.stepId().getBytes(StandardCharsets.UTF_8);

        if (jobIdBytes.length > 0xFFFF) {
            throw new IllegalArgumentException("jobId vượt quá 65535 byte: " + jobIdBytes.length);
        }
        if (stepIdBytes.length > 0xFFFF) {
            throw new IllegalArgumentException("stepId vượt quá 65535 byte: " + stepIdBytes.length);
        }

        int totalLen = 16                          // traceId
                + 2 + jobIdBytes.length             // jobIdLen + jobId
                + 2 + stepIdBytes.length            // stepIdLen + stepId
                + 4 + p.payload().length;           // payloadLen + payload

        ByteBuffer buf = ByteBuffer.allocate(totalLen);

        buf.putLong(p.traceId().getMostSignificantBits());
        buf.putLong(p.traceId().getLeastSignificantBits());

        buf.putShort((short) jobIdBytes.length);
        buf.put(jobIdBytes);

        buf.putShort((short) stepIdBytes.length);
        buf.put(stepIdBytes);

        buf.putInt(p.payload().length);
        buf.put(p.payload());

        return buf.array();
    }

    public static AssignStepPayload decodeAssignStepPayload(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        long msb = buf.getLong();
        long lsb = buf.getLong();
        UUID traceId = new UUID(msb, lsb);

        int jobIdLen = Short.toUnsignedInt(buf.getShort()); // & 0xFFFF để tránh đọc ra số âm
        byte[] jobIdBytes = new byte[jobIdLen];
        buf.get(jobIdBytes);
        String jobId = new String(jobIdBytes, StandardCharsets.UTF_8);

        int stepIdLen = Short.toUnsignedInt(buf.getShort());
        byte[] stepIdBytes = new byte[stepIdLen];
        buf.get(stepIdBytes);
        String stepId = new String(stepIdBytes, StandardCharsets.UTF_8);

        int payloadLen = buf.getInt();
        byte[] payload = new byte[payloadLen];
        buf.get(payload);

        return new AssignStepPayload(traceId, jobId, stepId, payload);
    }

    // ---------- Tầng trong: StepResultPayload <-> byte[] ----------

    public static byte[] encodeStepResultPayload(StepResultPayload p) {
        byte[] jobIdBytes = p.jobId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] stepIdBytes = p.stepId().getBytes(java.nio.charset.StandardCharsets.UTF_8);

        if (jobIdBytes.length > 0xFFFF) {
            throw new IllegalArgumentException("jobId vượt quá 65535 byte: " + jobIdBytes.length);
        }
        if (stepIdBytes.length > 0xFFFF) {
            throw new IllegalArgumentException("stepId vượt quá 65535 byte: " + stepIdBytes.length);
        }

        int totalLen = 16                          // traceId
                + 2 + jobIdBytes.length             // jobIdLen + jobId
                + 2 + stepIdBytes.length            // stepIdLen + stepId
                + 4 + p.result().length;           // resultLen + result

        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(totalLen);

        buf.putLong(p.traceId().getMostSignificantBits());
        buf.putLong(p.traceId().getLeastSignificantBits());

        buf.putShort((short) jobIdBytes.length);
        buf.put(jobIdBytes);

        buf.putShort((short) stepIdBytes.length);
        buf.put(stepIdBytes);

        buf.putInt(p.result().length);
        buf.put(p.result());

        return buf.array();
    }

    public static StepResultPayload decodeStepResultPayload(byte[] bytes) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);

        long msb = buf.getLong();
        long lsb = buf.getLong();
        java.util.UUID traceId = new java.util.UUID(msb, lsb);

        int jobIdLen = java.lang.Short.toUnsignedInt(buf.getShort());
        byte[] jobIdBytes = new byte[jobIdLen];
        buf.get(jobIdBytes);
        String jobId = new String(jobIdBytes, java.nio.charset.StandardCharsets.UTF_8);

        int stepIdLen = java.lang.Short.toUnsignedInt(buf.getShort());
        byte[] stepIdBytes = new byte[stepIdLen];
        buf.get(stepIdBytes);
        String stepId = new String(stepIdBytes, java.nio.charset.StandardCharsets.UTF_8);

        int resultLen = buf.getInt();
        byte[] result = new byte[resultLen];
        buf.get(result);

        return new StepResultPayload(traceId, jobId, stepId, result);
    }
}