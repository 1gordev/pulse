package com.id.pulse.modules.datapoints.service;

import com.id.pulse.modules.datapoints.model.DpAccumulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DpAccumulatorsManagerTest {

    private DpAccumulatorsManager manager;
    private static final String GROUP = "group1";

    @BeforeEach
    void setUp() {
        manager = new DpAccumulatorsManager();
    }

    @Test
    void shouldCreateAndRetrieveSameAccumulator() {
        DpAccumulator acc1 = manager.getOrCreate(GROUP, "path/1", 1000, 2000);
        DpAccumulator acc2 = manager.getOrCreate(GROUP, "path/1", 1000, 2000);

        assertSame(acc1, acc2, "Expected to retrieve the same instance");
    }

    @Test
    void shouldFindCompletedAccumulators() {
        manager.getOrCreate(GROUP, "path/1", 1000, 1500);
        manager.getOrCreate(GROUP, "path/1", 2000, 3000);
        manager.getOrCreate(GROUP, "path/2", 1000, 1500); // different path

        List<DpAccumulator> result = manager.findCompleted(GROUP,"path/1", 2000);

        assertEquals(1, result.size());
        assertTrue(result.stream().allMatch(acc -> acc.getPath().equals("path/1")));
    }

    @Test
    void shouldThrowOnNullPathInFindCompleted() {
        assertThrows(IllegalArgumentException.class, () -> manager.findCompleted(GROUP, null, 1000));
    }

    @Test
    void shouldRemoveAccumulators() {
        DpAccumulator a1 = manager.getOrCreate(GROUP, "path/1", 1000, 2000);
        DpAccumulator a2 = manager.getOrCreate(GROUP, "path/1", 3000, 4000);

        manager.remove(List.of(a1));

        List<DpAccumulator> result = manager.findCompleted(GROUP,"path/1", 5000);

        assertEquals(1, result.size());
        assertEquals(a2, result.getFirst());
    }

    @Test
    void shouldThrowOnNullRemoveList() {
        assertThrows(IllegalArgumentException.class, () -> manager.remove(null));
    }

    @Test
    void shouldHandleConcurrentGetOrCreate() throws Exception {
        int threadCount = 10;
        String path = "concurrent/path";
        long start = 5000;
        long end = 6000;

        Set<DpAccumulator> instances;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Future<DpAccumulator>> futures = new java.util.ArrayList<>();
            Callable<DpAccumulator> task = () -> manager.getOrCreate(GROUP, path, start, end);

            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(task));
            }

            instances = new HashSet<>();
            for (Future<DpAccumulator> future : futures) {
                instances.add(future.get());
            }
        }

        assertEquals(1, instances.size(), "Expected only one instance to be created under concurrent access");
    }

    @Test
    void shouldHandleConcurrentRemove() throws Exception {
        int threadCount = 10;
        String path = "concurrent/remove";
        long start = 7000;
        long end = 8000;

        // Prepopulate with multiple accumulators
        DpAccumulator acc1 = manager.getOrCreate(GROUP, path, start, end);
        DpAccumulator acc2 = manager.getOrCreate(GROUP, path, start + 100, end + 100);

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Future<?>> futures = new java.util.ArrayList<>();
            Callable<Void> removeTask = () -> {
                manager.remove(List.of(acc1, acc2));
                return null;
            };

            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(removeTask));
            }

            // Await all tasks and ensure no exceptions
            for (Future<?> future : futures) {
                future.get();
            }
        }

        // After all removals, accumulators should be gone
        List<DpAccumulator> result = manager.findCompleted(GROUP, "path/1", end + 200);
        assertTrue(result.isEmpty(), "Expected all accumulators to be removed under concurrent removeAll calls");
    }
}
