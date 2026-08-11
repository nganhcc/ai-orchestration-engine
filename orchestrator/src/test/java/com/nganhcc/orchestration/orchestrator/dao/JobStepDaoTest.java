package com.nganhcc.orchestration.orchestrator.dao;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class JobStepDaoTest {

    static class FakeUpdater implements JobStepDao.Updater {
        public String lastSql;
        public Object[] lastArgs;
        public int toReturn = 1;

        @Override
        public int update(String sql, Object... args) {
            this.lastSql = sql;
            this.lastArgs = args;
            return toReturn;
        }
    }

    @Test
    void updateStatusIfEpochAtMost_returnsTrue_whenRowsAffected() {
        FakeUpdater fake = new FakeUpdater();
        JobStepDao dao = new JobStepDao(fake);

        UUID stepId = UUID.randomUUID();
        boolean updated = dao.updateStatusIfEpochAtMost(stepId, 5L, "DONE", "{}");

        assertTrue(updated);
        assertNotNull(fake.lastSql);
        assertTrue(fake.lastSql.contains("UPDATE job_step"));
        assertEquals(5L, fake.lastArgs[2]); // epoch param
        assertEquals(stepId, fake.lastArgs[3]);
    }

    @Test
    void updateStatusIfEpochAtMost_returnsFalse_whenNoRows() {
        FakeUpdater fake = new FakeUpdater();
        fake.toReturn = 0;
        JobStepDao dao = new JobStepDao(fake);

        boolean updated = dao.updateStatusIfEpochAtMost(UUID.randomUUID(), 1L, "DONE", "{}");
        assertFalse(updated);
    }
}
