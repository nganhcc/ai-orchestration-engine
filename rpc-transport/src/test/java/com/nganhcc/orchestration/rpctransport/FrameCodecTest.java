package com.nganhcc.orchestration.rpctransport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FrameCodecTest {

    @Nested
    class FrameEncodeDecode {

        @Test
        void heartbeat_payloadRong_roundTrip() {
            FrameMessage original = new FrameMessage(FrameMessage.HEARTBEAT, 1001L, 7L, new byte[0]);
            byte[] wire = FrameCodec.encodeFrame(original);

            assertEquals(21, wire.length); // 4 (length) + 1 + 8 + 8 + 0
            assertEquals(17, readInt(wire, 0)); // bodyLength khai báo đúng

            FrameMessage decoded = FrameCodec.decodeFrame(wire);
            assertEquals(original, decoded);
        }

        @Test
        void assignStep_withPayload_byteLevelCorrect() {
            AssignStepPayload payload = new AssignStepPayload(
                    UUID.randomUUID(), "job-42", "step-1", "{}".getBytes(StandardCharsets.UTF_8));
            byte[] payloadBytes = FrameCodec.encodeAssignStepPayload(payload);
            assertEquals(38, payloadBytes.length); // 16+2+6+2+6+4+2

            FrameMessage original = new FrameMessage(FrameMessage.ASSIGN_STEP, 1L, 7L, payloadBytes);
            byte[] wire = FrameCodec.encodeFrame(original);

            assertEquals(59, wire.length);       // 4 + 55
            assertEquals(55, readInt(wire, 0));  // 1+8+8+38

            FrameMessage decoded = FrameCodec.decodeFrame(wire);
            assertEquals(original, decoded);

            AssignStepPayload decodedPayload = FrameCodec.decodeAssignStepPayload(decoded.payload());
            assertEquals(payload, decodedPayload);
        }

        @Test
        void decodeBody_khongCoLengthField_dungKhiDaBiNettyStrip() {
            FrameMessage original = new FrameMessage(FrameMessage.ACK, 5L, 1L, new byte[0]);
            byte[] fullWire = FrameCodec.encodeFrame(original);
            byte[] bodyOnly = java.util.Arrays.copyOfRange(fullWire, 4, fullWire.length);

            assertEquals(original, FrameCodec.decodeBody(bodyOnly));
        }

        @Test
        void totalLength_khaiManSaiSoByteThuc_nemException() {
            byte[] corrupted = {0, 0, 0, 100, 1, 2, 3};
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> FrameCodec.decodeFrame(corrupted));
            assertTrue(ex.getMessage().contains("100"));
        }

        @Test
        void payloadLon_100KB_khongTran() {
            byte[] bigPayload = new byte[100_000];
            java.util.Arrays.fill(bigPayload, (byte) 'x');
            FrameMessage original = new FrameMessage(FrameMessage.STEP_RESULT, 1L, 1L, bigPayload);

            byte[] wire = FrameCodec.encodeFrame(original);
            FrameMessage decoded = FrameCodec.decodeFrame(wire);

            assertArrayEquals(bigPayload, decoded.payload());
        }

        private int readInt(byte[] b, int offset) {
            return ((b[offset] & 0xFF) << 24) | ((b[offset + 1] & 0xFF) << 16)
                    | ((b[offset + 2] & 0xFF) << 8) | (b[offset + 3] & 0xFF);
        }
    }

    @Nested
    class AssignStepPayloadEncodeDecode {

        @Test
        void jobId_coKyTuTiengViet_dungByteLengthChuKhongPhaiCharLength() {
            AssignStepPayload payload = new AssignStepPayload(
                    UUID.randomUUID(), "công-việc-1", "step-1", new byte[0]);
            byte[] encoded = FrameCodec.encodeAssignStepPayload(payload);
            AssignStepPayload decoded = FrameCodec.decodeAssignStepPayload(encoded);

            assertEquals(payload.jobId(), decoded.jobId());
            // "công-việc-1" nhiều hơn số byte so với số ký tự vì có dấu -> assert rõ khác nhau
            assertNotEquals(payload.jobId().length(),
                    payload.jobId().getBytes(StandardCharsets.UTF_8).length);
        }

        @Test
        void jobIdLen_gan32768_dungUnsignedKhongBiDocThanhSoAm() {
            // short có dấu: giá trị 40000 nếu đọc sai (không toUnsignedInt) sẽ ra số âm
            String longJobId = "a".repeat(40_000);
            AssignStepPayload payload = new AssignStepPayload(
                    UUID.randomUUID(), longJobId, "step-1", new byte[0]);

            byte[] encoded = FrameCodec.encodeAssignStepPayload(payload);
            AssignStepPayload decoded = FrameCodec.decodeAssignStepPayload(encoded);

            assertEquals(40_000, decoded.jobId().length());
        }

        @Test
        void jobId_vuotQua65535Byte_nemException() {
            String tooLong = "a".repeat(70_000);
            AssignStepPayload payload = new AssignStepPayload(
                    UUID.randomUUID(), tooLong, "step-1", new byte[0]);

            assertThrows(IllegalArgumentException.class,
                    () -> FrameCodec.encodeAssignStepPayload(payload));
        }

        @Test
        void traceId_giuNguyenChinhXacQuaRoundTrip() {
            UUID traceId = UUID.randomUUID();
            AssignStepPayload payload = new AssignStepPayload(traceId, "j", "s", new byte[0]);

            byte[] encoded = FrameCodec.encodeAssignStepPayload(payload);
            AssignStepPayload decoded = FrameCodec.decodeAssignStepPayload(encoded);

            assertEquals(traceId, decoded.traceId());
        }
    }
}