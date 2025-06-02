package com.id.pulse.utils;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.model.PulseDataPoint;

import java.util.*;

public class PulseDataMatrixBuilder {

    private final PulseDataMatrix matrix = new PulseDataMatrix();

    /**
     * Adds a single data point. This method is synchronized to ensure thread safety.
     *
     * @param groupCode the group code identifier
     * @param path      the path identifier
     * @param tms       the timestamp
     * @param value     the value at tms
     */
    public synchronized PulseDataMatrixBuilder add(String groupCode, String path, Long tms, Object value) {
        if (matrix.getData() == null) {
            matrix.setData(new LinkedHashMap<>());
        }
        matrix.getData().computeIfAbsent(groupCode, k -> new LinkedHashMap<>());
        matrix.getData().get(groupCode).computeIfAbsent(path, k -> new TreeMap<>());
        matrix.getData().get(groupCode).get(path).put(tms, value);

        return this;
    }

    /**
     * Returns the built PulseDataMatrix. (No further changes should be made after calling build.)
     *
     * @return the PulseDataMatrix built so far
     */
    public synchronized PulseDataMatrix build() {
        return matrix;
    }

    /**
     * Adds multiple data points corresponding to the given groupCode and path.
     * The sizes of the timestamps and values lists must match.
     *
     * @param groupCode  the group code identifier
     * @param path       the path identifier
     * @param timestamps the list of timestamps
     * @param values     the list of corresponding values
     * @param <T>        the type of the values
     */
    public synchronized <T> PulseDataMatrixBuilder addValues(String groupCode, String path, List<Long> timestamps, List<T> values) {
        if (timestamps.size() != values.size()) {
            throw new IllegalArgumentException("The sizes of timestamps and values must match.");
        }
        matrix.getData().computeIfAbsent(groupCode, k -> new LinkedHashMap<>());
        matrix.getData().get(groupCode).computeIfAbsent(path, k -> new TreeMap<>());
        var timeSeries = matrix.getData().get(groupCode).get(path);

        for (int i = 0; i < timestamps.size(); i++) {
            timeSeries.put(timestamps.get(i), values.get(i));
        }

        return this;
    }

    /**
     * Merges a list of PulseDataMatrix instances into this builder's matrix.
     * Each matrix is assumed to have its data stored as:
     * Map&lt;groupCode, Map&lt;path, TreeMap&lt;Long, Object&gt;&gt;&gt;
     *
     * @param matrices the list of PulseDataMatrix objects to merge
     * @return this builder
     */
    public synchronized PulseDataMatrixBuilder addMatrices(List<PulseDataMatrix> matrices) {
        if (matrices == null) {
            return this;
        }
        for (PulseDataMatrix m : matrices) {
            if (m.getData() != null) {
                // Iterate over each group in the matrix.
                m.getData().keySet().forEach(groupCode -> {
                    // Iterate over each path in the group.
                    m.getData().get(groupCode).keySet().forEach(path -> {
                        Map<Long, Object> timeSeries = m.getData().get(groupCode).get(path);
                        // Convert the timestamps and values to lists.
                        List<Long> timestamps = new ArrayList<>(timeSeries.keySet());
                        List<Object> values = new ArrayList<>(timeSeries.values());
                        // Use the typed addAll() to merge them.
                        this.addValues(groupCode, path, timestamps, values);
                    });
                });
            }
        }
        return this;
    }

    /**
     * Adds a list of PulseDataPoint objects to the matrix.
     *
     * @param deps the list of PulseDataPoint objects to add
     * @return this builder
     */
    public synchronized PulseDataMatrixBuilder addSparsePoints(List<PulseDataPoint> deps) {
        if (deps == null) {
            return this;
        }

        for (PulseDataPoint dep : deps) {
            add(dep.getGroupCode(), dep.getPath(), dep.getTms(), dep.getVal());
        }

        return this;
    }
}
