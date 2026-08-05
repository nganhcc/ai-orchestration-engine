package com.nganhcc.orchestration.raftcore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotTest {

    @Test
    void serializeDeserialize_roundTrip() {
        byte[] payload = "hello-snapshot".getBytes();
        Snapshot s = new Snapshot(123L, 7L, payload);
        byte[] ser = s.serialize();
        Snapshot d = Snapshot.deserialize(ser);

        assertEquals(123L, d.lastIncludedIndex);
        assertEquals(7L, d.lastIncludedTerm);
        assertArrayEquals(payload, d.data);
    }
}
