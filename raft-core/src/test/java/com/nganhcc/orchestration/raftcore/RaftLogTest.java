package com.nganhcc.orchestration.raftcore;

import org.junit.jupiter.api.Test;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    @Test
    void logRongThiLastIndexVaLastTermLaZero() {
        RaftLog log = new RaftLog();
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.lastTerm());
        assertEquals(0, log.size());
    }

    @Test
    void appendNewTuTangIndexTuanTu() {
        RaftLog log = new RaftLog();
        LogEntry e1 = log.appendNew(1, "A".getBytes());
        LogEntry e2 = log.appendNew(1, "B".getBytes());
        LogEntry e3 = log.appendNew(2, "C".getBytes());

        assertEquals(1, e1.index());
        assertEquals(2, e2.index());
        assertEquals(3, e3.index());
        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());
        assertEquals(3, log.size());
    }

    @Test
    void getEntryTraVeDungEntryTheoIndex() {
        RaftLog log = new RaftLog();
        log.appendNew(1, "A".getBytes());
        LogEntry e2 = log.appendNew(1, "B".getBytes());

        Optional<LogEntry> found = log.getEntry(2);
        assertTrue(found.isPresent());
        assertEquals(e2, found.get());

        assertTrue(log.getEntry(99).isEmpty(), "Index không tồn tại phải trả về empty");
        assertTrue(log.getEntry(0).isEmpty(), "Index=0 không phải entry thật, phải trả về empty");
    }

    @Test
    void termAtTraVeDungTermTheoIndex() {
        RaftLog log = new RaftLog();
        log.appendNew(1, "A".getBytes());
        log.appendNew(2, "B".getBytes());

        assertEquals(0, log.termAt(0), "Index=0 la quy uoc truoc entry dau tien, term phai la 0");
        assertEquals(1, log.termAt(1));
        assertEquals(2, log.termAt(2));
        assertEquals(0, log.termAt(99), "Index chua co entry phai tra ve 0");
    }

    // ---- Case 1: truncateFrom ----

    @Test
    void truncateFromXoaDungCacEntryTuIndexChiDinh() {
        RaftLog log = new RaftLog();
        for (int i = 0; i < 5; i++) {
            log.appendNew(1, ("cmd" + i).getBytes());
        }
        assertEquals(5, log.size()); // index 1..5

        log.truncateFrom(3); // xoá index >= 3, giữ lại index 1,2

        assertEquals(2, log.size());
        assertEquals(2, log.lastIndex());
        assertTrue(log.getEntry(1).isPresent());
        assertTrue(log.getEntry(2).isPresent());
        assertTrue(log.getEntry(3).isEmpty());
        assertTrue(log.getEntry(4).isEmpty());
        assertTrue(log.getEntry(5).isEmpty());
    }

    @Test
    void truncateFromTuIndexLonHonLastIndexKhongLamGiCa() {
        RaftLog log = new RaftLog();
        log.appendNew(1, "A".getBytes());
        log.appendNew(1, "B".getBytes());

        log.truncateFrom(10); // không có gì để xoá

        assertEquals(2, log.size());
        assertEquals(2, log.lastIndex());
    }

    @Test
    void truncateFromIndexNhoHon1NemException() {
        RaftLog log = new RaftLog();
        assertThrows(IllegalArgumentException.class, () -> log.truncateFrom(0));
    }

    // ---- Case 2: appendOrOverwrite - idempotent ----

    @Test
    void appendOrOverwriteGuiLaiEntryCuKhongLamGiCa() {
        RaftLog log = new RaftLog();
        LogEntry entry = new LogEntry(1, 1, "A".getBytes());
        log.appendOrOverwrite(entry);

        // giả lập network gửi lại đúng entry này lần 2 (mất ACK, leader retry)
        log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes()));

        assertEquals(1, log.size(), "Gửi lại entry cùng index cùng term không được tạo thêm bản ghi");
        assertEquals(entry, log.getEntry(1).get());
    }

    // ---- Case 3: appendOrOverwrite - conflict truncate ----

    @Test
    void appendOrOverwriteEntryCungIndexKhacTermThiTruncateRoiGhiDe() {
        RaftLog log = new RaftLog();
        log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes()));
        log.appendOrOverwrite(new LogEntry(1, 2, "B".getBytes()));
        log.appendOrOverwrite(new LogEntry(1, 3, "C".getBytes())); // entry "rác" từ leader cũ
        assertEquals(3, log.size());

        // leader mới (term=2) ghi đè tại index=3
        LogEntry newEntry = new LogEntry(2, 3, "C_moi".getBytes());
        log.appendOrOverwrite(newEntry);

        assertEquals(3, log.size(), "Chỉ ghi đè tại index=3, không thêm entry mới");
        assertEquals(newEntry, log.getEntry(3).get());
        assertEquals(2, log.termAt(3));
    }

    @Test
    void appendOrOverwriteConflictOGiuaThiXoaCaCacEntrySauNo() {
        RaftLog log = new RaftLog();
        log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes()));
        log.appendOrOverwrite(new LogEntry(1, 2, "B".getBytes()));
        log.appendOrOverwrite(new LogEntry(1, 3, "C".getBytes()));
        log.appendOrOverwrite(new LogEntry(1, 4, "D".getBytes()));
        assertEquals(4, log.size());

        // conflict xảy ra ngay tại index=2 (term khác) -> phải xoá luôn cả index 3,4 phía sau
        LogEntry newEntry2 = new LogEntry(2, 2, "B_moi".getBytes());
        log.appendOrOverwrite(newEntry2);

        assertEquals(2, log.size(), "Entry index 3,4 (thuộc nhánh cũ) phải bị xoá theo");
        assertEquals(newEntry2, log.getEntry(2).get());
        assertTrue(log.getEntry(3).isEmpty());
        assertTrue(log.getEntry(4).isEmpty());
    }

    @Test
    void appendOrOverwriteTiepNoiBinhThuongKhiIndexLaKeTiep() {
        RaftLog log = new RaftLog();
        log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes()));

        log.appendOrOverwrite(new LogEntry(1, 2, "B".getBytes())); // index=2, đúng bằng size()+1

        assertEquals(2, log.size());
        assertEquals(2, log.lastIndex());
    }

    @Test
    void appendOrOverwriteBiGapNemException() {
        RaftLog log = new RaftLog();
        log.appendOrOverwrite(new LogEntry(1, 1, "A".getBytes()));

        // cố ghi thẳng index=3 trong khi log mới chỉ có tới index=1 -> lỗi logic của caller
        LogEntry gapEntry = new LogEntry(1, 3, "C".getBytes());
        assertThrows(IllegalStateException.class, () -> log.appendOrOverwrite(gapEntry));
    }

    @Test
    void testCompactUpTo() {
        RaftLog log = new RaftLog();
        log.appendNew(1, "cmd1".getBytes());
        log.appendNew(1, "cmd2".getBytes());
        log.appendNew(2, "cmd3".getBytes());

        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());

        // Compact up to index 2, term 1
        log.compactUpTo(2, 1);

        assertEquals(2, log.getSnapshotOffset());
        assertEquals(1, log.getSnapshotOffsetTerm());
        assertEquals(1, log.size());
        assertEquals(3, log.lastIndex());
        assertEquals(2, log.lastTerm());

        // Check index 1 and 2 are compacted
        assertEquals(0, log.termAt(1));
        assertEquals(1, log.termAt(2)); // snapshotOffsetTerm
        assertEquals(2, log.termAt(3));

        assertTrue(log.getEntry(1).isEmpty());
        assertTrue(log.getEntry(2).isEmpty());
        assertTrue(log.getEntry(3).isPresent());

        // Append new entry after compact
        log.appendNew(2, "cmd4".getBytes());
        assertEquals(4, log.lastIndex());
        assertEquals(2, log.termAt(4));

        // Truncate from 4 should succeed
        log.truncateFrom(4);
        assertEquals(3, log.lastIndex());

        // Truncate from index <= snapshotOffset should throw
        assertThrows(IllegalArgumentException.class, () -> log.truncateFrom(2));
    }
}