package com.id.pulse.modules.alarms.service;

import com.id.pulse.modules.alarms.PulseAlarmChange;
import com.id.pulse.modules.alarms.PulseAlarmChangesReq;
import com.id.pulse.modules.alarms.model.PulseAlarmChangeEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers(disabledWithoutDocker = true)
class AlarmChangesCrudServiceTest {

    @Container
    static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:8.0");

    @DynamicPropertySource
    static void setMongoUri(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
    }

    @Autowired
    MongoTemplate mongoTemplate;
    @Autowired
    AlarmChangesCrudService service;

    @BeforeEach
    void setup() {
        mongoTemplate.dropCollection("PulseAlarmChange");

        // Insert multiple records for all combinations
        mongoTemplate.save(PulseAlarmChangeEntity.builder()
                .id("1")
                .alarmPath("A")
                .tms(Instant.now().minusSeconds(3000).toEpochMilli())
                .direction(true)
                .build(), "PulseAlarmChange");

        mongoTemplate.save(PulseAlarmChangeEntity.builder()
                .id("2")
                .alarmPath("A")
                .tms(Instant.now().minusSeconds(1000).toEpochMilli())
                .direction(false)
                .build(), "PulseAlarmChange");

        mongoTemplate.save(PulseAlarmChangeEntity.builder()
                .id("3")
                .alarmPath("B")
                .tms(Instant.now().minusSeconds(2000).toEpochMilli())
                .direction(true)
                .build(), "PulseAlarmChange");

        mongoTemplate.save(PulseAlarmChangeEntity.builder()
                .id("4")
                .alarmPath("C")
                .tms(Instant.now().toEpochMilli())
                .direction(false)
                .build(), "PulseAlarmChange");
    }

    @Test
    void testFindFiltered_allPaths_false_latestOnly_false_paths_timeWindow() {
        // Should return only "A" in range
        PulseAlarmChangesReq req = PulseAlarmChangesReq.builder()
                .allPaths(false)
                .paths(List.of("A"))
                .start(Instant.now().minusSeconds(2500))
                .end(Instant.now())
                .build();

        var result = service.findFiltered(req);
        assertEquals(1, result.size());
        assertEquals("2", result.getFirst().getId());
    }

    @Test
    void testFindFiltered_allPaths_true_latestOnly_false_timeWindow() {
        // Should return all records in time window (excluding id "1" which is oldest)
        PulseAlarmChangesReq req = PulseAlarmChangesReq.builder()
                .allPaths(true)
                .start(Instant.now().minusSeconds(2500))
                .end(Instant.now().plusSeconds(10))
                .build();

        var result = service.findFiltered(req);
        // IDs 2, 3, 4 in time window
        var ids = result.stream().map(PulseAlarmChange::getId).toList();
        assertTrue(ids.containsAll(List.of("2", "3", "4")));
        assertEquals(3, result.size());
    }

    @Test
    void testFindFiltered_allPaths_false_latestOnly_true_paths() {
        // Should return the latest for "A" and "B"
        PulseAlarmChangesReq req = PulseAlarmChangesReq.builder()
                .allPaths(false)
                .latestOnly(true)
                .paths(List.of("A", "B"))
                .build();

        var result = service.findFiltered(req);
        // Latest of A is id "2", of B is "3"
        var ids = result.stream().map(PulseAlarmChange::getId).toList();
        assertTrue(ids.containsAll(List.of("2", "3")));
        assertEquals(2, result.size());
    }

    @Test
    void testFindFiltered_allPaths_true_latestOnly_true() {
        // Should return the latest for each path: A ("2"), B ("3"), C ("4")
        PulseAlarmChangesReq req = PulseAlarmChangesReq.builder()
                .allPaths(true)
                .latestOnly(true)
                .build();

        var result = service.findFiltered(req);
        var ids = result.stream().map(PulseAlarmChange::getId).toList();
        assertTrue(ids.containsAll(List.of("2", "3", "4")));
        assertEquals(3, result.size());
    }

    @Test
    void testFindFiltered_no_results_in_time_window() {
        // Time window before all records
        PulseAlarmChangesReq req = PulseAlarmChangesReq.builder()
                .allPaths(true)
                .start(Instant.now().minusSeconds(10000))
                .end(Instant.now().minusSeconds(5000))
                .build();

        var result = service.findFiltered(req);
        assertTrue(result.isEmpty());
    }

    @Test
    void testFindFiltered_paths_empty() {
        // paths empty and allPaths=false should return nothing
        PulseAlarmChangesReq req = PulseAlarmChangesReq.builder()
                .allPaths(false)
                .paths(List.of())
                .start(Instant.now().minusSeconds(2500))
                .end(Instant.now())
                .build();

        var result = service.findFiltered(req);
        assertTrue(result.isEmpty());
    }

    @Test
    void testFindFiltered_latestOnly_paths_empty() {
        // latestOnly and paths empty and allPaths=false, expect nothing
        PulseAlarmChangesReq req = PulseAlarmChangesReq.builder()
                .allPaths(false)
                .latestOnly(true)
                .paths(List.of())
                .build();

        var result = service.findFiltered(req);
        assertTrue(result.isEmpty());
    }
}
