package com.id.pulse.modules.replay.service;

import com.id.pulse.config.AppConfig;
import com.id.pulse.modules.datapoints.model.PulseChunk;
import com.id.pulse.modules.datapoints.model.PulseChunkMetadataEntity;
import com.id.pulse.modules.replay.model.ReplayBatchCleanupResult;
import com.id.px3.rest.security.JwtService;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.client.RestTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReplayBatchCleanupServiceTest {

    @Mock
    private MongoTemplate mongoTemplate;
    @Mock
    private MongoDatabase mongoDatabase;
    @Mock
    private MongoCollection<Document> mongoCollection;
    @Mock
    private RestTemplate restTemplate;
    @Mock
    private JwtService jwtService;
    @Mock
    private AppConfig appConfig;
    @Mock
    private FindIterable<Document> findIterable;

    @Test
    void batchExistsReturnsFalseForBlankBatchId() {
        ReplayBatchCleanupService service = new ReplayBatchCleanupService(
                mongoTemplate,
                restTemplate,
                jwtService,
                appConfig
        );
        assertFalse(service.batchExists(" "));
    }

    @Test
    void batchExistsReturnsTrueWhenAnyCollectionMatches() {
        ReplayBatchCleanupService service = new ReplayBatchCleanupService(
                mongoTemplate,
                restTemplate,
                jwtService,
                appConfig
        );
        PulseChunkMetadataEntity metadata = new PulseChunkMetadataEntity();
        metadata.setCollectionName("col1");
        when(mongoTemplate.findAll(PulseChunkMetadataEntity.class)).thenReturn(List.of(metadata));
        when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection("col1")).thenReturn(mongoCollection);
        when(mongoCollection.countDocuments(any(Bson.class))).thenReturn(2L);

        assertTrue(service.batchExists("batch-1"));
    }

    @Test
    void deleteBatchRemovesPointsAndDeletesEmptyDocs() {
        ReplayBatchCleanupService service = new ReplayBatchCleanupService(
                mongoTemplate,
                restTemplate,
                jwtService,
                appConfig
        );
        when(appConfig.getIrisBackendBaseUrl()).thenReturn("");

        PulseChunkMetadataEntity metadata = new PulseChunkMetadataEntity();
        metadata.setCollectionName("col1");
        when(mongoTemplate.findAll(PulseChunkMetadataEntity.class)).thenReturn(List.of(metadata));
        when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection("col1")).thenReturn(mongoCollection);

        Document docUpdate = new Document("_id", "doc1")
                .append(PulseChunk.TS, List.of(1L, 2L, 3L))
                .append(PulseChunk.V, List.of(10, 20, 30))
                .append(PulseChunk.BATCH_IDS, List.of("batch-1", "keep", "batch-1"));
        Document docDelete = new Document("_id", "doc2")
                .append(PulseChunk.TS, List.of(4L, 5L))
                .append(PulseChunk.V, List.of(40, 50))
                .append(PulseChunk.BATCH_IDS, List.of("batch-1", "batch-1"));

        var cursor = org.mockito.Mockito.mock(com.mongodb.client.MongoCursor.class);
        when(cursor.hasNext()).thenReturn(true, true, false);
        when(cursor.next()).thenReturn(docUpdate, docDelete);
        when(findIterable.iterator()).thenReturn(cursor);
        when(mongoCollection.find(any(Bson.class))).thenReturn(findIterable);

        ReplayBatchCleanupResult result = service.deleteBatch("batch-1");

        assertEquals(4, result.pointsRemoved());
        assertEquals(1, result.docsUpdated());
        assertEquals(1, result.docsDeleted());
    }
}
