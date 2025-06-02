package com.id.pulse.modules.measures.service;

import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseMeasureEntity;
import com.id.px3.crud.logic.PxDefaultCrudServiceMongo;
import com.id.px3.crud.logic.PxDefaultMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Service
@Slf4j
public class MeasuresCrudService extends PxDefaultCrudServiceMongo<PulseMeasure, PulseMeasureEntity, String> {

    private final MongoTemplate mongoTemplate;

    public MeasuresCrudService(MongoTemplate mongoTemplate) {
        super(mongoTemplate,
                new PxDefaultMapper<>(PulseMeasure.class, PulseMeasureEntity.class),
                PxDefaultCrudServiceMongo.DEFAULT_COLLECTION_NAME);
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<PulseMeasure> processAction(String name, Map<String, Object> params) {
        return List.of();
    }

    /**
     * Find all measures by their paths.
     *
     * @param paths List of measure paths to search for.
     * @return List of PulseMeasure objects matching the given paths.
     */
    public List<PulseMeasure> findByPaths(List<String> paths) {
        return mongoTemplate.find(query(where(PulseMeasureEntity.PATH).in(paths)), getEntityClass(), getCollectionName()).stream()
                .map(getMapper()::toModel)
                .toList();
    }

    /**
     * Find a measure by its path.
     *
     * @param path The path of the measure to find.
     * @return An Optional containing the PulseMeasure if found, or empty if not found.
     */
    public Optional<PulseMeasure> findByPath(String path) {
        return Optional.ofNullable(mongoTemplate.findOne(query(where(PulseMeasureEntity.PATH).is(path)), getEntityClass(), getCollectionName()))
                .map(getMapper()::toModel);
    }

    /**
     * Find all measures whose details.physicalAssetId list contains the given ID.
     */
    public List<PulseMeasure> findAllReferringPhysicalAsset(String physicalAssetId) {
        // Fetch the raw entity documents
        List<PulseMeasureEntity> entities = mongoTemplate.find(query(where("details.physicalAssetId").is(physicalAssetId)), PulseMeasureEntity.class);

        // Convert to your domain model (assuming you have a converter method)
        return entities.stream()
                .map(getMapper()::toModel)
                .toList();
    }
}
