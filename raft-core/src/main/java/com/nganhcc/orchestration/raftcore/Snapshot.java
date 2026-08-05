package com.nganhcc.orchestration.raftcore;

import java.io.*;

public final class Snapshot {
    public final long lastIncludedIndex;
    public final long lastIncludedTerm;
    public final byte[] data;

    public Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.data = data == null ? new byte[0] : data.clone();
    }

    public byte[] serialize() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeLong(lastIncludedIndex);
            out.writeLong(lastIncludedTerm);
            out.writeInt(data.length);
            out.write(data);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Snapshot deserialize(byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream in = new DataInputStream(bais)) {
            long idx = in.readLong();
            long term = in.readLong();
            int len = in.readInt();
            byte[] data = new byte[len];
            in.readFully(data);
            return new Snapshot(idx, term, data);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
