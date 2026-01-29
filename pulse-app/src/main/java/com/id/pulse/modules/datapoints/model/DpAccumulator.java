package com.id.pulse.modules.datapoints.model;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.enums.PulseAggregationType;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Data
public class DpAccumulator {

    private final String groupCode;
    private final String path;
    private final long tmsAccStart;
    private final long tmsAccEnd;
    private final List<PulseDataPoint> dps = Collections.synchronizedList(new ArrayList<>());

    public DpAccumulator(String groupCode, String path, long tmsAccStart, long tmsAccEnd) {
        this.groupCode = groupCode;
        this.path = path;
        this.tmsAccStart = tmsAccStart;
        this.tmsAccEnd = tmsAccEnd;
    }

    /**
     * Add a data point to the accumulator. Thread-safe via synchronizedList.
     */
    public void push(PulseDataPoint dp) {
        dps.add(dp);
    }

    /**
     * Aggregate the collected data points according to the given aggregation type.
     * Supports DOUBLE and LONG for SUM/AVG; MIN/MAX/COPY work with Comparable values including BOOLEAN and STRING.
     */
    public PulseDataPoint aggregate(PulseAggregationType aggType) {
        synchronized (dps) {
            if (dps.isEmpty()) {
                return null;
            }

            PulseDataType type = dps.getFirst().getType();
            return switch (aggType) {
                case COPY -> dps.getLast();
                case MIN -> aggregateMin(type);
                case MAX -> aggregateMax(type);
                case SUM -> aggregateSum(type);
                case AVG -> aggregateAvg(type);
                default -> throw new IllegalArgumentException("Unsupported aggregation type: " + aggType);
            };
        }
    }

    private PulseDataPoint aggregateAvg(PulseDataType type) {
        double avg = switch (type) {
            case DOUBLE -> dps.stream()
                    .mapToDouble(dp -> ((Number) dp.getVal()).doubleValue())
                    .average().orElse(0.0);
            case LONG -> dps.stream()
                    .mapToLong(dp -> ((Number) dp.getVal()).longValue())
                    .average().orElse(0.0);
            default -> throw new IllegalArgumentException("AVG not supported for type: " + type);
        };
        return PulseDataPoint.builder()
                .groupCode(groupCode)
                .path(path)
                .tms(tmsAccEnd)
                .type(PulseDataType.DOUBLE)
                .val(avg)
                .batchId(resolveBatchId())
                .build();
    }

    private PulseDataPoint aggregateSum(PulseDataType type) {
        var builder = PulseDataPoint.builder()
                .groupCode(groupCode)
                .path(path)
                .tms(tmsAccEnd)
                .batchId(resolveBatchId());

        switch (type) {
            case DOUBLE -> {
                double sum = dps.stream()
                        .mapToDouble(dp -> ((Number) dp.getVal()).doubleValue())
                        .sum();
                return builder.type(PulseDataType.DOUBLE).val(sum).build();
            }
            case LONG -> {
                long sum = dps.stream()
                        .mapToLong(dp -> ((Number) dp.getVal()).longValue())
                        .sum();
                return builder.type(PulseDataType.LONG).val(sum).build();
            }
            default -> throw new IllegalArgumentException("SUM not supported for type: " + type);
        }
    }

    private <T extends Comparable<T>> PulseDataPoint aggregateMin(PulseDataType dataType) {
        @SuppressWarnings("unchecked")
        T minValue = dps.stream()
                .map(dp -> (T) dp.getVal())
                .min(Comparator.naturalOrder())
                .orElse(null);
        return PulseDataPoint.builder()
                .groupCode(groupCode)
                .path(path)
                .tms(tmsAccEnd)
                .type(dataType)
                .val(minValue)
                .batchId(resolveBatchId())
                .build();
    }

    private <T extends Comparable<T>> PulseDataPoint aggregateMax(PulseDataType dataType) {
        @SuppressWarnings("unchecked")
        T maxValue = dps.stream()
                .map(dp -> (T) dp.getVal())
                .max(Comparator.naturalOrder())
                .orElse(null);
        return PulseDataPoint.builder()
                .groupCode(groupCode)
                .path(path)
                .tms(tmsAccEnd)
                .type(dataType)
                .val(maxValue)
                .batchId(resolveBatchId())
                .build();
    }

    private String resolveBatchId() {
        return dps.stream()
                .max(Comparator.comparingLong(PulseDataPoint::getTms))
                .map(PulseDataPoint::getBatchId)
                .orElse(null);
    }
}
