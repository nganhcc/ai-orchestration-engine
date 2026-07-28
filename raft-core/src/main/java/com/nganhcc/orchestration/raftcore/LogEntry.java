package com.nganhcc.orchestration.raftcore;

import java.util.Arrays;

public final class LogEntry {
    private final long term;
    private final long index;
    private final byte[] command;

    public LogEntry(long term, long index, byte[] command) {
        if (term <0 ){
            throw new IllegalArgumentException("Term khong duoc nho hon 0");
        }
        if (index<1){
            throw new IllegalArgumentException("Index khong duoc nho hon 1");
        }
        this.term = term;
        this.index= index;
        this.command = command==null? new byte[0] : command.clone();
    }

    public long term(){
        return this.term;
    }
    public long index(){
        return this.index;
    }
    public byte[] command(){
        return this.command.clone();
    }

    @Override
    public boolean equals(Object o){
        if (this == o)return true;
        if (!(o instanceof LogEntry other))return false;
        return this.term == other.term && this.index == other.index && Arrays.equals(this.command, other.command);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(term);
        result = 31 * result + Long.hashCode(index);
        result = 31 * result + Arrays.hashCode(command);
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry{term=" + term + ", index=" + index + ", commandLen=" + command.length + "}";
    }
}
