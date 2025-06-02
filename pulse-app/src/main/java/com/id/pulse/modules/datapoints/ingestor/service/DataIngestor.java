package com.id.pulse.modules.datapoints.ingestor.service;

import com.id.pulse.config.AppConfig;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.datapoints.model.PulseChunk;
import com.id.pulse.modules.datapoints.model.PulseChunkMetadataEntity;
import com.id.pulse.modules.timeseries.model.PulseIngestorWriteResult;
import com.id.pulse.modules.timeseries.model.PulseChunkMetadata;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Service
@Slf4j
public class DataIngestor {

    private final AppConfig appConfig;
    private final MongoTemplate mongoTemplate;

    // This task queue logs a warning when the number of enqueued tasks exceeds the given warningThreshold
    static class WarningLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {

        private final int warningThreshold;
        private final long warningIntervalMillis;
        private final AtomicLong lastWarningTime = new AtomicLong(0);

        public WarningLinkedBlockingQueue(int capacity, int warningThreshold, long warningIntervalMillis) {
            super(capacity);
            this.warningThreshold = warningThreshold;
            this.warningIntervalMillis = warningIntervalMillis;
        }

        @Override
        public boolean offer(E e) {
            int currentSize = size();
            long now = System.currentTimeMillis();
            if (currentSize >= warningThreshold && now - lastWarningTime.get() >= warningIntervalMillis) {
                if (lastWarningTime.compareAndSet(lastWarningTime.get(), now)) {
                    log.warn("Task queue size is %d (>= %d)".formatted(currentSize, warningThreshold));
                }
            }
            return super.offer(e);
        }
    }

    private final ExecutorService executor;


    public DataIngestor(AppConfig appConfig, MongoTemplate mongoTemplate) {
        this.appConfig = appConfig;
        this.mongoTemplate = mongoTemplate;

        executor = new ThreadPoolExecutor(
                appConfig.getIngestorWriteThreads(),
                appConfig.getIngestorWriteThreads(),
                0L, TimeUnit.MILLISECONDS,
                new WarningLinkedBlockingQueue<>(appConfig.getIngestorQueueSize(), appConfig.getIngestorQueueSize() / 2, 60000),
                Thread.ofVirtual().factory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @Transactional
    public PulseChunkMetadata prepareMetadata(String groupCode, String path, PulseDataType type, long samplingRate) {
        // Check params
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("Type cannot be null");
        }
        if (samplingRate <= 1) {
            throw new IllegalArgumentException("Sampling rate must be greater than one");
        }

        // Logic to prepare metadata
        log.trace("Preparing DataIngestor for path:%s, type:%s, samplingRate:%d".formatted(path, type, samplingRate));

        // Clean and sanitize path and type
        String safePath = path.replaceAll("\\s+", "").replaceAll("[^a-zA-Z0-9._-]", "_");
        String safeType = type.name().replaceAll("\\s+", "").replaceAll("[^a-zA-Z0-9._-]", "_");

        // Generate collection name and ID
        String collectionName = "PulseChunks_%s_%s_%d".formatted(groupCode, safeType, samplingRate);
        String metadataId = "%s_%s_%s_%d".formatted(groupCode, safePath, safeType, samplingRate);

        // Upsert metadata entry - Upsert's here to guarantee atomicity and idempotency
        Criteria criteria = new Criteria().andOperator(
                where(PulseChunkMetadataEntity.SAFE_PATH).is(safePath),
                where(PulseChunkMetadataEntity.TYPE).is(type),
                where(PulseChunkMetadataEntity.SAMPLING_RATE).is(samplingRate)
        );
        Update update = new Update()
                .setOnInsert(PulseChunkMetadataEntity.ID, metadataId)
                .setOnInsert(PulseChunkMetadataEntity.GROUP_CODE, groupCode)
                .setOnInsert(PulseChunkMetadataEntity.SAFE_PATH, safePath)
                .setOnInsert(PulseChunkMetadataEntity.PATH, path)
                .setOnInsert(PulseChunkMetadataEntity.TYPE, type)
                .setOnInsert(PulseChunkMetadataEntity.SAMPLING_RATE, samplingRate)
                .setOnInsert(PulseChunkMetadataEntity.COLLECTION_NAME, collectionName);
        mongoTemplate.upsert(query(criteria), update, PulseChunkMetadataEntity.class);

        // Read, map to model and return
        var entity = mongoTemplate.findOne(query(criteria), PulseChunkMetadataEntity.class);
        if (entity == null) {
            throw new IllegalStateException("Failed to retrieve metadata entry");
        }
        var model = new PulseChunkMetadata();
        BeanUtils.copyProperties(entity, model);
        return model;
    }

    public CompletableFuture<PulseIngestorWriteResult> writeAsync(PulseChunkMetadata metadata, Map<Long, Object> timeSeries) {
        return CompletableFuture.supplyAsync(() -> writeSync(metadata, timeSeries), executor);
    }

    private PulseIngestorWriteResult writeSync(PulseChunkMetadata metadata, Map<Long, Object> timeSeries) {
        // Put data into chunks. Create them if they don't exist
        return switch (metadata.getType()) {
            case DOUBLE ->
                    writeSyncTyped(new LinkedHashMap<String, PulseChunk<Double>>(), metadata, metadata.getType(), timeSeries);
            case LONG ->
                    writeSyncTyped(new LinkedHashMap<String, PulseChunk<Long>>(), metadata, metadata.getType(), timeSeries);
            case STRING ->
                    writeSyncTyped(new LinkedHashMap<String, PulseChunk<String>>(), metadata, metadata.getType(), timeSeries);
            case BOOLEAN ->
                    writeSyncTyped(new LinkedHashMap<String, PulseChunk<Boolean>>(), metadata, metadata.getType(), timeSeries);
        };
    }

    private <T> PulseIngestorWriteResult writeSyncTyped(Map<String, PulseChunk<T>> chunkMap,
                                                        PulseChunkMetadata metadata,
                                                        PulseDataType dataType,
                                                        Map<Long, Object> timeSeries) {

        // Fill chunks
        for (Map.Entry<Long, Object> entry : timeSeries.entrySet()) {
            long ts = entry.getKey();
            T value = getTypedValue(dataType, entry.getValue());

            if (ts <= 0) {
                log.warn("Ignoring data point with non-positive timestamp: %d".formatted(ts));
                continue;
            }

            if (value != null) {
                // Calculate chunk size in terms of milliseconds
                long chunkTimeRange = appConfig.getIngestorChunkSize() * metadata.getSamplingRate();

                // Normalize ts according to samplingRate
                // This could arithmetically to negative nTs, but we can ignore it since
                // we are writing 'real' timestamps, that are always significantly greater than both 0 and samplingRate
                long nTs = ts - (ts % metadata.getSamplingRate());

                // Calculate tsStart and tsEnd (tsStart <= ts < tsEnd (== tsStart + chunkTimeRange)
                long tsStart = nTs - (nTs % chunkTimeRange);
                long tsEnd = tsStart + chunkTimeRange;

                // Determine the chunk ID based on the timestamp and metadata
                String chunkId = "%s_%s_%d_%d".formatted(metadata.getSafePath(), dataType.name(), tsStart, metadata.getSamplingRate());

                // Create or update the chunk
                PulseChunk<T> chunk = chunkMap.computeIfAbsent(chunkId, id -> buildNewChunk(metadata, dataType, id, tsStart, tsEnd));

                // Update the start and end timestamps
                // nTs cannot be outside the range of [tsStart, tsEnd] here
                if (nTs < chunk.getTsStart() || nTs >= chunk.getTsEnd()) {
                    log.warn("nTs '%d' out of range [%d, %d]... This should not happen".formatted(nTs, chunk.getTsStart(), chunk.getTsEnd()));
                } else {
                    // Update the chunk with the new data point
                    chunk.getTs().add(nTs);
                    chunk.getV().add(value);
                }
            }
        }

        // Persist non-void chunks
        final long[] points = {0};
        final Duration[] writeDuration = {Duration.ZERO};
        chunkMap.values().stream()
                .filter(chunk -> !chunk.getTs().isEmpty())
                .forEach(chunk -> {
                    Instant writeStart = Instant.now();

                    Update update = new Update()
                            .setOnInsert(PulseChunk.ID, chunk.getId())
                            .setOnInsert(PulseChunk.PATH, chunk.getPath())
                            .setOnInsert(PulseChunk.TS_START, chunk.getTsStart())
                            .setOnInsert(PulseChunk.TS_END, chunk.getTsEnd())
                            .setOnInsert(PulseChunk.DATA_TYPE, chunk.getDataType())
                            .push(PulseChunk.TS).each(chunk.getTs())
                            .push(PulseChunk.V).each(chunk.getV());

                    mongoTemplate.upsert(
                            query(Criteria.where(PulseChunk.ID).is(chunk.getId())),
                            update,
                            metadata.getCollectionName());


                    points[0] += chunk.getTs().size();

                    writeDuration[0] = writeDuration[0].plus(Duration.between(writeStart, Instant.now()));
                });

        // Create a write result for this chunk
        return PulseIngestorWriteResult.builder()
                .chunks(chunkMap.size())
                .points(points[0])
                .duration(writeDuration[0])
                .build();
    }

    private <T> PulseChunk<T> buildNewChunk(PulseChunkMetadata metadata, PulseDataType dataType, String id, long tsStart, long tsEnd) {
        var newChunk = switch (dataType) {
            case DOUBLE -> new PulseChunk<Double>();
            case LONG -> new PulseChunk<Long>();
            case STRING -> new PulseChunk<String>();
            case BOOLEAN -> new PulseChunk<Boolean>();
        };

        newChunk.setId(id);
        newChunk.setPath(metadata.getSafePath());
        newChunk.setTsStart(tsStart);
        newChunk.setTsEnd(tsEnd);
        newChunk.setDataType(dataType);
        newChunk.setTs(new ArrayList<>());
        newChunk.setV(new ArrayList<>());

        // This is safe because we are using the type as instructed from dataType param
        //noinspection unchecked
        return (PulseChunk<T>) newChunk;
    }

    private <T> T getTypedValue(PulseDataType dataType, Object untypedValue) {
        return switch (dataType) {
            case DOUBLE -> {
                if (untypedValue instanceof Double) {
                    //noinspection unchecked
                    yield (T) untypedValue;
                } else {
                    yield null;
                }
            }

            case LONG -> {
                if (untypedValue instanceof Long) {
                    //noinspection unchecked
                    yield (T) untypedValue;
                } else {
                    yield null;
                }
            }

            case STRING -> {
                if (untypedValue instanceof String) {
                    //noinspection unchecked
                    yield (T) untypedValue;
                } else {
                    yield null;
                }
            }

            case BOOLEAN -> {
                if (untypedValue instanceof Boolean) {
                    //noinspection unchecked
                    yield (T) untypedValue;
                } else {
                    yield null;
                }
            }
        };
    }

}
