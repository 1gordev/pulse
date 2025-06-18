package com.id.pulse.modules.datapoints.extractor.logic;

import com.id.pulse.config.AppConfig;
import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.channel.service.ChannelGroupsCrudService;
import com.id.pulse.modules.channel.service.ChannelsCrudService;
import com.id.pulse.modules.datapoints.model.PulseChunk;
import com.id.pulse.modules.datapoints.service.ChunkMetadataCrudService;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.service.MeasuresCrudService;
import com.id.pulse.modules.timeseries.model.PulseChunkMetadata;
import com.id.pulse.utils.PulseDataMatrixBuilder;
import com.id.px3.utils.ThrowingFn;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class DataExtractorProcessor {

    public static final String MEASURES_GROUP = "_MEASURES_";

    private final AppConfig appConfig;
    private final ChannelsCrudService channelsCrudService;
    private final ChannelGroupsCrudService channelGroupsCrudService;
    private final ChunkMetadataCrudService chunkMetadataCrudService;
    private final MeasuresCrudService measuresCrudService;
    private final MongoTemplate mongoTemplate;

    public DataExtractorProcessor(AppConfig appConfig,
                                  ChannelsCrudService channelsCrudService,
                                  ChannelGroupsCrudService channelGroupsCrudService,
                                  ChunkMetadataCrudService chunkMetadataCrudService,
                                  MongoClient mongoClient, MeasuresCrudService measuresCrudService,
                                  MongoTemplate mongoTemplate) {

        this.appConfig = appConfig;
        this.channelsCrudService = channelsCrudService;
        this.channelGroupsCrudService = channelGroupsCrudService;
        this.chunkMetadataCrudService = chunkMetadataCrudService;
        this.measuresCrudService = measuresCrudService;
        this.mongoTemplate = mongoTemplate;
    }

    public PulseDataMatrix extract(List<String> paths, Instant tsReadStart, Instant tsReadEnd) {
        if (paths == null || paths.isEmpty() || tsReadStart == null || tsReadEnd == null) {
            throw new IllegalArgumentException("Invalid input parameters.");
        }

        // Find channels and groups
        var channelsMap = channelsCrudService.findByPaths(paths).stream().collect(Collectors.toMap(PulseChannel::getPath, ch -> ch));
        var channelGroupCodes = channelsMap.values().stream().map(PulseChannel::getChannelGroupCode).collect(Collectors.toSet());
        var channelGroupsMap = channelGroupsCrudService.findByCodes(new ArrayList<>(channelGroupCodes)).stream().collect(Collectors.toMap(PulseChannelGroup::getCode, gr -> gr));

        // Find channels metadata
        var channelMetadataMap = chunkMetadataCrudService.findByPaths(paths).stream().collect(Collectors.toMap(PulseChunkMetadata::getPath, meta -> meta));

        // Process group by group
        List<CompletableFuture<PulseDataMatrix>> channelsTasks;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            channelsTasks = channelGroupsMap.values().stream().map(gr ->
                    CompletableFuture.supplyAsync(() -> extractChannelGroup(gr, channelsMap, channelMetadataMap, tsReadStart, tsReadEnd), executor)
            ).toList();
        }

        // Find measures and groups
        var measuresMap = measuresCrudService.findByPaths(paths).stream().collect(Collectors.toMap(PulseMeasure::getPath, ch -> ch));

        // Find measures metadata
        var measureMetadataMap = chunkMetadataCrudService.findByPaths(paths).stream().collect(Collectors.toMap(PulseChunkMetadata::getPath, meta -> meta));

        // Process group by group
        CompletableFuture<PulseDataMatrix> measuresTask;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            measuresTask = CompletableFuture.supplyAsync(() -> extractMeasureGroup(measuresMap, measureMetadataMap, tsReadStart, tsReadEnd), executor);
        }

        // Merge matrix
        return PulseDataMatrix.builder()
                .addMatrices(channelsTasks.stream().map(ThrowingFn.mayThrow(CompletableFuture::get)).toList())
                .addMatrices(Stream.of(measuresTask).map(ThrowingFn.mayThrow(CompletableFuture::get)).toList())
                .build();
    }

    private PulseDataMatrix extractChannelGroup(PulseChannelGroup gr,
                                                Map<String, PulseChannel> channelsMap,
                                                Map<String, PulseChunkMetadata> metaMap,
                                                Instant tsReadStart,
                                                Instant tsReadEnd) {
        // Split the channels list into smaller batches (up to AppConfig.extractorReadThreads)
        // and process each batch in parallel
        List<CompletableFuture<PulseDataMatrix>> tasks = new ArrayList<>();
        for (int batchStart = 0; batchStart < channelsMap.size(); batchStart += appConfig.getExtractorReadThreads()) {
            int batchEnd = Math.min(batchStart + appConfig.getExtractorReadThreads(), channelsMap.size());
            List<PulseChannel> channelsBatch = new ArrayList<>(channelsMap.values()).subList(batchStart, batchEnd);
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                tasks.add(CompletableFuture.supplyAsync(() -> extractChannelBatch(gr, channelsBatch, metaMap, tsReadStart, tsReadEnd), executor));
            }
        }
        return PulseDataMatrix.builder()
                .addMatrices(tasks.stream().map(ThrowingFn.mayThrow(CompletableFuture::get)).toList())
                .build();
    }

    private PulseDataMatrix extractMeasureGroup(Map<String, PulseMeasure> measuresMap,
                                                Map<String, PulseChunkMetadata> metaMap,
                                                Instant tsReadStart,
                                                Instant tsReadEnd) {
        // Split the measures list into smaller batches (up to AppConfig.extractorReadThreads)
        // and process each batch in parallel
        List<CompletableFuture<PulseDataMatrix>> tasks = new ArrayList<>();
        for (int batchStart = 0; batchStart < measuresMap.size(); batchStart += appConfig.getExtractorReadThreads()) {
            int batchEnd = Math.min(batchStart + appConfig.getExtractorReadThreads(), measuresMap.size());
            List<PulseMeasure> measuresBatch = new ArrayList<>(measuresMap.values()).subList(batchStart, batchEnd);
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                tasks.add(CompletableFuture.supplyAsync(() -> extractMeasureBatch(measuresBatch, metaMap, tsReadStart, tsReadEnd), executor));
            }
        }
        return PulseDataMatrix.builder()
                .addMatrices(tasks.stream().map(ThrowingFn.mayThrow(CompletableFuture::get)).toList())
                .build();
    }

    private PulseDataMatrix extractChannelBatch(PulseChannelGroup gr,
                                                List<PulseChannel> channelsBatch,
                                                Map<String, PulseChunkMetadata> metaMap,
                                                Instant tsReadStart,
                                                Instant tsReadEnd) {

        long chunkMillis = gr.getInterval() * appConfig.getIngestorChunkSize();
        long tsReadStartMillis = tsReadStart.toEpochMilli();
        long tsReadStartNorm = (tsReadStartMillis / chunkMillis) * chunkMillis;
        long tsReadEndMillis = tsReadEnd.toEpochMilli();
        long tsReadEndNorm = ((tsReadEndMillis + chunkMillis - 1) / chunkMillis) * chunkMillis;

        // Get collections
        var collections = channelsBatch.stream()
                .map(ch -> metaMap.get(ch.getPath()))
                .filter(Objects::nonNull)
                .map(PulseChunkMetadata::getCollectionName)
                .collect(Collectors.toSet());

        // Query each collection
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<PulseDataMatrix>> tasks = collections.stream()
                    .map(collectionName -> CompletableFuture.supplyAsync(() -> queryCollection(
                            gr.getCode(),
                            collectionName,
                            channelsBatch.stream().map(PulseChannel::getPath).collect(Collectors.toSet()),
                            metaMap,
                            tsReadStartNorm,
                            tsReadEndNorm
                    ), executor))
                    .toList();

            return PulseDataMatrix.builder()
                    .addMatrices(tasks.stream().map(ThrowingFn.mayThrow(CompletableFuture::get)).toList())
                    .build();
        }
    }

    private PulseDataMatrix extractMeasureBatch(List<PulseMeasure> measuresBatch,
                                                Map<String, PulseChunkMetadata> metaMap,
                                                Instant tsReadStart,
                                                Instant tsReadEnd) {

        long chunkMillis = 1000L;
        long tsReadStartMillis = tsReadStart.toEpochMilli();
        long tsReadStartNorm = (tsReadStartMillis / chunkMillis) * chunkMillis;
        long tsReadEndMillis = tsReadEnd.toEpochMilli();
        long tsReadEndNorm = ((tsReadEndMillis + chunkMillis - 1) / chunkMillis) * chunkMillis;

        // Get collections
        var collections = measuresBatch.stream()
                .map(ch -> metaMap.get(ch.getPath()))
                .filter(Objects::nonNull)
                .map(PulseChunkMetadata::getCollectionName)
                .collect(Collectors.toSet());

        // Query each collection
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<PulseDataMatrix>> tasks = collections.stream()
                    .map(collectionName -> CompletableFuture.supplyAsync(() -> queryCollection(
                            MEASURES_GROUP,
                            collectionName,
                            measuresBatch.stream().map(PulseMeasure::getPath).collect(Collectors.toSet()),
                            metaMap,
                            tsReadStartNorm,
                            tsReadEndNorm
                    ), executor))
                    .toList();

            return PulseDataMatrix.builder()
                    .addMatrices(tasks.stream().map(ThrowingFn.mayThrow(CompletableFuture::get)).toList())
                    .build();
        }
    }

    private PulseDataMatrix queryCollection(String groupCode,
                                            String collectionName,
                                            Set<String> paths,
                                            Map<String, PulseChunkMetadata> metaMap,
                                            long tsReadStartNorm,
                                            long tsReadEndNorm) {

        MongoDatabase database = mongoTemplate.getDb();
        MongoCollection<Document> collection = database.getCollection(collectionName);
        PulseDataMatrixBuilder matrixBuilder = PulseDataMatrix.builder();

        Bson query = Filters.and(
                Filters.in(PulseChunk.PATH, paths),
                Filters.gte(PulseChunk.TS_START, tsReadStartNorm),
                Filters.lte(PulseChunk.TS_END, tsReadEndNorm)
        );

        for (Document doc : collection.find(query)) {
            String path = doc.getString(PulseChunk.PATH);
            List<Long> timestamps = doc.getList(PulseChunk.TS, Long.class, Collections.emptyList());
            if (timestamps.isEmpty() || timestamps.getFirst() < tsReadStartNorm || timestamps.getLast() >= tsReadEndNorm) {
                throw new IllegalStateException("Invalid timestamps in data chunk.");
            }

            var values = switch (metaMap.get(path).getType()) {
                case DOUBLE -> doc.getList(PulseChunk.V, Double.class, Collections.emptyList());
                case LONG -> doc.getList(PulseChunk.V, Long.class, Collections.emptyList());
                case BOOLEAN -> doc.getList(PulseChunk.V, Boolean.class, Collections.emptyList());
                case STRING -> doc.getList(PulseChunk.V, String.class, Collections.emptyList());
            };

            matrixBuilder.addValues(groupCode, path, timestamps, values);
        }

        return matrixBuilder.build();
    }

}
