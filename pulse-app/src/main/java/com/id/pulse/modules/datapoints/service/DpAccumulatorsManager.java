package com.id.pulse.modules.datapoints.service;

import com.id.pulse.modules.datapoints.model.DpAccumulator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class DpAccumulatorsManager {

    private final Map<String, DpAccumulator> accumulators = Collections.synchronizedMap(new HashMap<>());

    /**
     * Creates a new DpAccumulator if it does not exist, or returns the existing one.
     *
     * @param groupCode - Group code of the accumulator
     * @param path - Path of the accumulator
     * @param tmsAccStart - Start time of the accumulator
     * @param tmsAccEnd - End time of the accumulator
     *
     * @return The DpAccumulator object
     */
    public DpAccumulator getOrCreate(String groupCode, String path, long tmsAccStart, long tmsAccEnd) {
        return accumulators.computeIfAbsent(
                makeKey(groupCode, path, tmsAccStart, tmsAccEnd),
                (k) -> new DpAccumulator(groupCode, path, tmsAccStart, tmsAccEnd)
        );
    }

    /**
     * Finds all accumulators that are completed (i.e., tmsAccEnd <= tmsCompare).
     *
     * @param path        - Path of the accumulator
     * @param tmsCompare  - Time to compare against
     *
     * @return List of completed accumulators
     */
    public List<DpAccumulator> findCompleted(String groupCode, String path, long tmsCompare) {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }

        // Take a snapshot of the values to avoid ConcurrentModificationException
        List<DpAccumulator> snapshot;
        synchronized (accumulators) {
            snapshot = List.copyOf(accumulators.values());
        }

        return snapshot.stream()
                .filter(acc -> acc.getGroupCode().equals(groupCode) && acc.getPath().equals(path) && acc.getTmsAccEnd() <= tmsCompare)
                .toList();
    }

    /**
     * Removes all accumulators that are in the removeList.
     *
     * @param removeList - List of DpAccumulator to remove
     */
    public void remove(List<DpAccumulator> removeList) {
        if (removeList == null) {
            throw new IllegalArgumentException("RemoveList cannot be null");
        }

        removeList.stream()
                .map(acc -> makeKey(acc.getGroupCode(), acc.getPath(), acc.getTmsAccStart(), acc.getTmsAccEnd()))
                .forEach(accumulators::remove);
    }

    private static String makeKey(String groupCode, String path, long tmsAccStart, long tmsAccEnd) {
        return "%s_%s_%d_%d".formatted(groupCode, path, tmsAccStart, tmsAccEnd);
    }
}
