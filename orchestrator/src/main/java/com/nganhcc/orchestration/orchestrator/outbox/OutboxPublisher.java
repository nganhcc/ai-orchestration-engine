package com.nganhcc.orchestration.orchestrator.outbox;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class OutboxPublisher {

    private final JdbcOperations jdbc;

    public OutboxPublisher(JdbcOperations jdbc) {
        this.jdbc = jdbc;
    }

    @Scheduled(fixedDelay = 2000)
    public void publishOutboxEvents() {
        String querySql = "SELECT event_id, aggregate_id, event_type, payload FROM outbox WHERE published = false ORDER BY created_at ASC LIMIT 10";
        List<Map<String, Object>> events = jdbc.queryForList(querySql);

        if (events.isEmpty()) {
            return;
        }

        System.out.printf("OutboxPublisher: Found %d unpublished events%n", events.size());

        String updateSql = "UPDATE outbox SET published = true WHERE event_id = ?";
        
        for (Map<String, Object> event : events) {
            UUID eventId = (UUID) event.get("event_id");
            UUID aggregateId = (UUID) event.get("aggregate_id");
            String eventType = (String) event.get("event_type");
            // jsonb columns come back from JDBC as PGobject, not String — use toString()
            Object payloadObj = event.get("payload");
            String payload = payloadObj == null ? null : payloadObj.toString();

            // Minimum viable logic: Just log the event as requested in phase4.md
            System.out.printf("Publishing event - ID: %s, Aggregate: %s, Type: %s, Payload: %s%n",
                    eventId, aggregateId, eventType, payload);

            jdbc.update(updateSql, eventId);
        }
    }
}
