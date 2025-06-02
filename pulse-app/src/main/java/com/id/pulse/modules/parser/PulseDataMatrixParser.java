package com.id.pulse.modules.parser;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.px3.utils.SafeConvert;

import java.util.*;
import java.util.stream.Collectors;

public class PulseDataMatrixParser {

    private final PulseDataMatrix matrix;

    private PulseDataMatrixParser(PulseDataMatrix matrix) {
        this.matrix = matrix;
    }

    public static PulseDataMatrixParser from(PulseDataMatrix matrix) {
        return new PulseDataMatrixParser(matrix);
    }

    public List<PulseDataPoint> filterByPath(String pathFilter) {
        // Collect  entries for pathFilter
        List<PulseDataPoint> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, Map<Long, Object>>> groupEntry : matrix.getData().entrySet()) {
            Map<String, Map<Long, Object>> byPath = groupEntry.getValue();
            Map<Long, Object> tmsMap = byPath.get(pathFilter);

            // Skip groups that don’t have this path
            if (tmsMap == null) {
                continue;
            }

            String groupCode = groupEntry.getKey();

            for (Map.Entry<Long, Object> entry : tmsMap.entrySet()) {
                long tms = entry.getKey();
                Object value = entry.getValue();
                result.add(PulseDataPoint.builder()
                        .groupCode(groupCode)
                        .path(pathFilter)
                        .tms(tms)
                        .type(detectType(value))
                        .val(value)
                        .build()
                );
            }
        }

        // Sort by time and return
        result.sort(Comparator.comparingLong(PulseDataPoint::getTms));
        return result;
    }

    public List<Map<Long, Object>> toTimeSeries(String pathFilter) {
        // Collect  entries for pathFilter
        List<Map<Long, Object>> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, Map<Long, Object>>> groupEntry : matrix.getData().entrySet()) {
            Map<String, Map<Long, Object>> byPath = groupEntry.getValue();
            Map<Long, Object> tmsMap = byPath.get(pathFilter);

            // Skip groups that don’t have this path
            if (tmsMap == null) {
                continue;
            }

            for (Map.Entry<Long, Object> entry : tmsMap.entrySet()) {
                long tms = entry.getKey();
                Object value = entry.getValue();
                result.add(Map.of(tms, value));
            }
        }

        return result;
    }

    /**
     * Returns a list of timestamps for the given path.
     *
     * @param path - The path to filter by
     * @return List of timestamps associated with the given path
     */
    public List<Long> toTimestamps(String path) {
        // Collect entries for pathFilter
        List<Long> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, Map<Long, Object>>> groupEntry : matrix.getData().entrySet()) {
            Map<String, Map<Long, Object>> byPath = groupEntry.getValue();
            Map<Long, Object> tmsMap = byPath.get(path);

            // Skip groups that don’t have this path
            if (tmsMap == null) {
                continue;
            }

            result.addAll(tmsMap.keySet());
        }

        return result;
    }

    /**
     * Merges timestamps from multiple paths into a single sorted list.
     *
     * @param paths - List of paths to merge timestamps from
     * @return Sorted list of unique timestamps across all provided paths
     */
    public List<Long> toMergedTimestamps(List<String> paths) {
        Set<Long> allTimestamps = new HashSet<>();
        for (String path : paths) {
            allTimestamps.addAll(toTimestamps(path));
        }
        List<Long> merged = new ArrayList<>(allTimestamps);
        Collections.sort(merged);
        return merged;
    }


    public List<Object> toValues(String pathFilter) {
        // Collect  entries for pathFilter
        List<Object> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, Map<Long, Object>>> groupEntry : matrix.getData().entrySet()) {
            Map<String, Map<Long, Object>> byPath = groupEntry.getValue();
            Map<Long, Object> tmsMap = byPath.get(pathFilter);

            // Skip groups that don’t have this path
            if (tmsMap == null) {
                continue;
            }

            result.addAll(tmsMap.values());
        }

        return result;
    }

    public List<Double> toNumbers(String pathFilter, Double defaultValue) {
        return toNumbers(pathFilter, defaultValue, null);
    }

    public List<Double> toNumbers(String pathFilter, Double defaultValue, List<Long> targetTimestamps) {
        List<Long> originalTimestamps = toTimestamps(pathFilter);
        List<Double> originalValues = toValues(pathFilter).stream()
                .map(o -> SafeConvert.toDouble(o).orElse(defaultValue))
                .collect(Collectors.toList());

        if (targetTimestamps == null || targetTimestamps.equals(originalTimestamps)) {
            return originalValues;
        }

        List<Double> aligned = new ArrayList<>(targetTimestamps.size());
        for (Long ts : targetTimestamps) {
            // Find index of nearest timestamp
            int nearestIdx = 0;
            long minDist = Long.MAX_VALUE;
            for (int i = 0; i < originalTimestamps.size(); i++) {
                long dist = Math.abs(originalTimestamps.get(i) - ts);
                if (dist < minDist) {
                    minDist = dist;
                    nearestIdx = i;
                }
            }
            Double value = originalValues.isEmpty() ? defaultValue : originalValues.get(nearestIdx);
            aligned.add(value);
        }
        return aligned;
    }

    public Double toLatestNumber(String pathFilter, Double defaultValue) {
        var numbers = toNumbers(pathFilter, defaultValue);
        if (numbers.isEmpty()) {
            return defaultValue;
        }
        return numbers.getLast();
    }

    public List<Boolean> toBooleans(String pathFilter, Boolean defaultValue) {
        return toBooleans(pathFilter, defaultValue, null);
    }

    public List<Boolean> toBooleans(String pathFilter, Boolean defaultValue, List<Long> targetTimestamps) {
        List<Long> originalTimestamps = toTimestamps(pathFilter);
        List<Boolean> originalValues = toValues(pathFilter).stream()
                .map(o -> SafeConvert.toBoolean(o).orElse(defaultValue))
                .collect(Collectors.toList());

        if (targetTimestamps == null || targetTimestamps.equals(originalTimestamps)) {
            return originalValues;
        }

        List<Boolean> aligned = new ArrayList<>(targetTimestamps.size());
        for (Long ts : targetTimestamps) {
            int nearestIdx = 0;
            long minDist = Long.MAX_VALUE;
            for (int i = 0; i < originalTimestamps.size(); i++) {
                long dist = Math.abs(originalTimestamps.get(i) - ts);
                if (dist < minDist) {
                    minDist = dist;
                    nearestIdx = i;
                }
            }
            Boolean value = originalValues.isEmpty() ? defaultValue : originalValues.get(nearestIdx);
            aligned.add(value);
        }
        return aligned;
    }

    public Boolean toLatestBoolean(String pathFilter, Boolean defaultValue) {
        var booleans = toBooleans(pathFilter, defaultValue);
        if (booleans.isEmpty()) {
            return defaultValue;
        }
        return booleans.getLast();
    }

    public List<String> toStrings(String pathFilter, String defaultValue) {
        return toStrings(pathFilter, defaultValue, null);
    }

    public List<String> toStrings(String pathFilter, String defaultValue, List<Long> targetTimestamps) {
        List<Long> originalTimestamps = toTimestamps(pathFilter);
        List<String> originalValues = toValues(pathFilter).stream()
                .map(o -> SafeConvert.toString(o).orElse(defaultValue))
                .collect(Collectors.toList());
        

        if (targetTimestamps == null || targetTimestamps.equals(originalTimestamps)) {
            return originalValues;
        }

        List<String> aligned = new ArrayList<>(targetTimestamps.size());
        for (Long ts : targetTimestamps) {
            int nearestIdx = 0;
            long minDist = Long.MAX_VALUE;
            for (int i = 0; i < originalTimestamps.size(); i++) {
                long dist = Math.abs(originalTimestamps.get(i) - ts);
                if (dist < minDist) {
                    minDist = dist;
                    nearestIdx = i;
                }
            }
            String value = originalValues.isEmpty() ? defaultValue : originalValues.get(nearestIdx);
            aligned.add(value);
        }
        return aligned;
    }

    public String toLatestString(String pathFilter, String defaultValue) {
        var Strings = toStrings(pathFilter, defaultValue);
        if (Strings.isEmpty()) {
            return defaultValue;
        }
        return Strings.getLast();
    }

    private PulseDataType detectType(Object value) {
        return switch (value) {
            case String ignored -> PulseDataType.STRING;
            case Integer ignored -> PulseDataType.LONG;
            case Long ignored -> PulseDataType.LONG;
            case Double ignored -> PulseDataType.DOUBLE;
            case Boolean ignored -> PulseDataType.BOOLEAN;
            case null, default -> PulseDataType.DOUBLE;
        };
    }
}
