package com.id.pulse.modules.datapoints.service;

import com.id.pulse.modules.datapoints.model.PulseChunkMetadataEntity;
import com.id.pulse.modules.timeseries.model.PulseChunkMetadata;
import com.id.px3.crud.logic.PxDefaultCrudServiceMongo;
import com.id.px3.crud.logic.PxDefaultMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Service
@Slf4j
public class ChunkMetadataCrudService extends PxDefaultCrudServiceMongo<PulseChunkMetadata, PulseChunkMetadataEntity, String> {

    private final MongoTemplate mongoTemplate;

    public ChunkMetadataCrudService(MongoTemplate mongoTemplate) {
        super(mongoTemplate,
                new PxDefaultMapper<>(PulseChunkMetadata.class, PulseChunkMetadataEntity.class),
                PxDefaultCrudServiceMongo.DEFAULT_COLLECTION_NAME);
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<PulseChunkMetadata> processAction(String name, Map<String, Object> params) {
        return List.of();
    }

    public List<PulseChunkMetadata> findByPaths(List<String> paths) {
        return mongoTemplate.find(query(where(PulseChunkMetadataEntity.PATH).in(paths)), getEntityClass(), getCollectionName()).stream()
                .map(getMapper()::toModel)
                .toList();
    }
}
