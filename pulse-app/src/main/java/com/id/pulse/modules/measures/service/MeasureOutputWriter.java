package com.id.pulse.modules.measures.service;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.datapoints.ingestor.service.DataIngestor;
import com.id.pulse.modules.measures.model.MeasureValueWriteRequest;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.poller.service.LatestValuesBucket;
import com.id.pulse.modules.timeseries.model.PulseChunkMetadata;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class MeasureOutputWriter {

    private static final long DEFAULT_INTERVAL_MS = 60_000L;

    private final MeasuresCrudService measuresCrudService;
    private final DataIngestor dataIngestor;
    private final LatestValuesBucket latestValuesBucket;

    private final ConcurrentHashMap<String, PulseChunkMetadata> metadataCache = new ConcurrentHashMap<>();

    public void writeValues(List<MeasureValueWriteRequest> requests) {
        if (CollectionUtils.isEmpty(requests)) {
            return;
        }

        requests.stream()
                .filter(Objects::nonNull)
                .forEach(this::writeSingleValue);
    }

    private void writeSingleValue(MeasureValueWriteRequest request) {
        if (!StringUtils.hasText(request.getPath())) {
            log.warn("Skipping bayesian output without path");
            return;
        }

        if (request.getValue() == null || request.getValue().isNaN()) {
            log.warn("Skipping bayesian output for {} due to null/NaN value", request.getPath());
            return;
        }

        Optional<PulseMeasure> measureOpt = measuresCrudService.findByPath(request.getPath());
        if (measureOpt.isEmpty()) {
            log.warn("No measure found for path {}", request.getPath());
            return;
        }

        PulseMeasure measure = measureOpt.get();
        PulseDataType dataType = Optional.ofNullable(measure.getDataType()).orElse(PulseDataType.DOUBLE);
        Object coercedValue = coerceValue(request.getValue(), dataType);
        if (coercedValue == null) {
            log.warn("Unable to coerce value {} for measure {} to type {}", request.getValue(), request.getPath(), dataType);
            return;
        }

        long timestamp = Optional.ofNullable(request.getTimestamp())
                .filter(ts -> ts > 0)
                .orElseGet(() -> Instant.now().toEpochMilli());

        // Prefer explicitly propagated sampling interval; fall back to measure validity; then to default.
        long interval = Optional.ofNullable(request.getIntervalMs())
                .filter(v -> v != null && v > 0)
                .or(() -> Optional.ofNullable(measure.getValidity()).filter(v -> v != null && v > 0))
                .orElse(DEFAULT_INTERVAL_MS);

        PulseDataPoint dp = PulseDataPoint.builder()
                .groupCode(MeasureTransformer.MEASURES_GROUP)
                .path(measure.getPath())
                .tms(timestamp)
                .type(dataType)
                .val(coercedValue)
                .batchId(request.getBatchId())
                .build();

        PulseChannelGroup virtualGroup = PulseChannelGroup.builder()
                .code(MeasureTransformer.MEASURES_GROUP)
                .interval(interval)
                .build();

        latestValuesBucket.writeDataPoint(List.of(virtualGroup), dp);

        String metadataCacheKey = "%s@%d".formatted(measure.getPath(), interval);
        PulseChunkMetadata metadata = metadataCache.computeIfAbsent(
                metadataCacheKey,
                key -> dataIngestor.prepareMetadata(
                        MeasureTransformer.MEASURES_GROUP,
                        measure.getPath(),
                        dataType,
                        interval
                )
        );

        Map<Long, Object> timeSeries = Map.of(dp.getTms(), coercedValue);
        Map<Long, String> batchIdsByTs = Map.of(dp.getTms(), dp.getBatchId());
        dataIngestor.writeAsync(metadata, timeSeries, batchIdsByTs)
                .exceptionally(ex -> {
                    log.error("Error writing bayesian output for {} to ingestor", measure.getPath(), ex);
                    return null;
                });
    }

    private Object coerceValue(Double value, PulseDataType type) {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case DOUBLE -> value;
            case LONG -> Math.round(value);
            case BOOLEAN -> value >= 0.5d;
            case STRING -> value.toString();
        };
    }
}
