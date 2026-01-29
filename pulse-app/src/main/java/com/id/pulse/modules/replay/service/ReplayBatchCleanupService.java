package com.id.pulse.modules.replay.service;

import com.id.pulse.config.AppConfig;
import com.id.pulse.modules.datapoints.model.PulseChunk;
import com.id.pulse.modules.datapoints.model.PulseChunkMetadataEntity;
import com.id.pulse.modules.replay.model.ReplayBatchCleanupResult;
import com.id.px3.model.DefaultRoles;
import com.id.px3.rest.security.JwtService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplayBatchCleanupService {

    private static final Duration TOKEN_TTL = Duration.ofMinutes(1);
    private static final String SUBJECT = "pulse-replay-cleanup";

    private final MongoTemplate mongoTemplate;
    private final RestTemplate restTemplate;
    private final JwtService jwtService;
    private final AppConfig appConfig;

    public boolean batchExists(String batchId) {
        if (!StringUtils.hasText(batchId)) {
            return false;
        }
        for (String collectionName : listCollections()) {
            try {
                MongoCollection<Document> collection = mongoTemplate.getDb().getCollection(collectionName);
                long count = collection.countDocuments(Filters.eq(PulseChunk.BATCH_IDS, batchId));
                if (count > 0) {
                    return true;
                }
            } catch (Exception ex) {
                log.warn("Batch existence check failed on {}", collectionName, ex);
            }
        }
        return false;
    }

    public ReplayBatchCleanupResult deleteBatch(String batchId) {
        if (!StringUtils.hasText(batchId)) {
            return new ReplayBatchCleanupResult(0, 0, 0);
        }
        int docsUpdated = 0;
        int docsDeleted = 0;
        int pointsRemoved = 0;

        MongoDatabase db = mongoTemplate.getDb();
        for (String collectionName : listCollections()) {
            MongoCollection<Document> collection = db.getCollection(collectionName);
            try (var cursor = collection.find(Filters.eq(PulseChunk.BATCH_IDS, batchId)).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    List<Long> ts = doc.getList(PulseChunk.TS, Long.class);
                    List<Object> v = doc.getList(PulseChunk.V, Object.class);
                    List<String> batchIds = doc.getList(PulseChunk.BATCH_IDS, String.class);
                    if (ts == null || v == null || batchIds == null) {
                        continue;
                    }
                    int size = Math.min(ts.size(), Math.min(v.size(), batchIds.size()));
                    if (size == 0) {
                        continue;
                    }
                    List<Long> nextTs = new ArrayList<>(size);
                    List<Object> nextV = new ArrayList<>(size);
                    List<String> nextBatchIds = new ArrayList<>(size);
                    int removed = 0;
                    for (int i = 0; i < size; i++) {
                        String id = batchIds.get(i);
                        if (batchId.equals(id)) {
                            removed++;
                            continue;
                        }
                        nextTs.add(ts.get(i));
                        nextV.add(v.get(i));
                        nextBatchIds.add(id);
                    }
                    if (removed == 0) {
                        continue;
                    }
                    pointsRemoved += removed;
                    Object docId = doc.get("_id");
                    if (nextTs.isEmpty()) {
                        collection.deleteOne(Filters.eq("_id", docId));
                        docsDeleted++;
                    } else {
                        collection.updateOne(
                                Filters.eq("_id", docId),
                                Updates.combine(
                                        Updates.set(PulseChunk.TS, nextTs),
                                        Updates.set(PulseChunk.V, nextV),
                                        Updates.set(PulseChunk.BATCH_IDS, nextBatchIds)
                                )
                        );
                        docsUpdated++;
                    }
                }
            } catch (Exception ex) {
                log.warn("Batch cleanup failed on {}", collectionName, ex);
            }
        }

        deleteBayesianOutputs(batchId);
        return new ReplayBatchCleanupResult(pointsRemoved, docsUpdated, docsDeleted);
    }

    private void deleteBayesianOutputs(String batchId) {
        String baseUrl = appConfig.getIrisBackendBaseUrl();
        if (!StringUtils.hasText(baseUrl)) {
            log.warn("IRIS backend base URL is not configured; skipping BN batch cleanup.");
            return;
        }
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(jwtService.generateToken(SUBJECT, Set.of(DefaultRoles.ROOT), TOKEN_TTL));
            HttpEntity<Void> request = new HttpEntity<>(headers);
            restTemplate.exchange(baseUrl + "/bnet/compute-output/batch/" + batchId, HttpMethod.DELETE, request, Void.class);
        } catch (Exception ex) {
            log.warn("Failed to delete BN outputs for batchId {}", batchId, ex);
        }
    }

    private Set<String> listCollections() {
        List<PulseChunkMetadataEntity> metadata = mongoTemplate.findAll(PulseChunkMetadataEntity.class);
        Set<String> collectionNames = new HashSet<>();
        for (PulseChunkMetadataEntity entry : metadata) {
            if (entry.getCollectionName() != null && !entry.getCollectionName().isBlank()) {
                collectionNames.add(entry.getCollectionName());
            }
        }
        return collectionNames;
    }
}
