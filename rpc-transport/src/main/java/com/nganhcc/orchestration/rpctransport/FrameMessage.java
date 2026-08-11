package com.nganhcc.orchestration.rpctransport;

/**
 * Biểu diễn 1 frame ở tầng ngoài cùng, chưa parse payload nghiệp vụ.
 * payload là byte[] thô — cấu trúc bên trong tuỳ theo messageType.
 */
public record FrameMessage(
        byte messageType,
        long requestId,
        long epoch,
        byte[] payload
) {
    public static final byte ASSIGN_STEP = 0x01;
    public static final byte STEP_RESULT = 0x02;
    public static final byte HEARTBEAT = 0x03;
    public static final byte ACK = 0x04;
    public static final byte STALE_LEADER_REJECT = 0x05;
    public static final byte RAFT_REQUEST_VOTE = 0x10;
    public static final byte RAFT_REQUEST_VOTE_RESPONSE = 0x11;
    public static final byte RAFT_APPEND_ENTRIES = 0x12;
    public static final byte RAFT_APPEND_ENTRIES_RESPONSE = 0x13;
    public static final byte RAFT_INSTALL_SNAPSHOT = 0x14;
    public static final byte RAFT_INSTALL_SNAPSHOT_RESPONSE = 0x15;

    // record tự sinh equals/hashCode dựa trên field, nhưng byte[] so sánh theo reference
    // -> override lại để test round-trip so sánh đúng nội dung, không phải địa chỉ mảng.
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FrameMessage other)) return false;
        return messageType == other.messageType
                && requestId == other.requestId
                && epoch == other.epoch
                && java.util.Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(messageType, requestId, epoch);
        result = 31 * result + java.util.Arrays.hashCode(payload);
        return result;
    }
}
