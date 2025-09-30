package com.id.pulse.modules.measures.service;

import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseMeasureEntity;
import com.id.px3.crud.logic.PxDefaultCrudServiceMongo;
import com.id.px3.crud.logic.PxDefaultMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    @Override
    public PulseMeasure save(PulseMeasure model) {
        var measure = super.save(model);
        syncAssetLinks(measure, null);
        return measure;
    }

    @Override
    public PulseMeasure update(String id, PulseMeasure model) {
        var previousState = findById(id);
        var measure = super.update(id, model);
        syncAssetLinks(measure, previousState);
        return measure;
    }

    @Override
    public void delete(String id) {
        var measure = findById(id);
        super.delete(id);
        removeAssetLinks(measure);
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
     * Find all measures whose details.physicalAssetIds list contains the given ID.
     */
    public List<PulseMeasure> findAllReferringPhysicalAsset(String physicalAssetId) {
        // Fetch the raw entity documents
        List<PulseMeasureEntity> entities = mongoTemplate.find(
                query(where("details.physicalAssetIds").is(physicalAssetId)),
                PulseMeasureEntity.class);

        // Convert to your domain model (assuming you have a converter method)
        return entities.stream()
                .map(getMapper()::toModel)
                .toList();
    }

    /**
     * Find all measures whose details.virtualAssetIds list contains the given ID.
     */
    public List<PulseMeasure> findAllReferringVirtualAsset(String virtualAssetId) {
        List<PulseMeasureEntity> entities = mongoTemplate.find(
                query(where("details.virtualAssetIds").is(virtualAssetId)),
                PulseMeasureEntity.class);

        return entities.stream()
                .map(getMapper()::toModel)
                .toList();
    }

    private void syncAssetLinks(PulseMeasure current, PulseMeasure previous) {
        if (current == null || current.getId() == null || current.getId().isBlank()) {
            return;
        }

        var newPhysical = extractIds(current, "physicalAssetIds");
        var oldPhysical = extractIds(previous, "physicalAssetIds");
        updateAssetCollectionLinks("PhysicalAsset", newPhysical, oldPhysical, current.getId());

        var newVirtual = extractIds(current, "virtualAssetIds");
        var oldVirtual = extractIds(previous, "virtualAssetIds");
        updateAssetCollectionLinks("VirtualAsset", newVirtual, oldVirtual, current.getId());
    }

    private void removeAssetLinks(PulseMeasure measure) {
        if (measure == null || measure.getId() == null || measure.getId().isBlank()) {
            return;
        }
        pullMeasureIdFromAssets("PhysicalAsset", extractIds(measure, "physicalAssetIds"), measure.getId());
        pullMeasureIdFromAssets("VirtualAsset", extractIds(measure, "virtualAssetIds"), measure.getId());
    }

    private Set<String> extractIds(PulseMeasure measure, String key) {
        if (measure == null || measure.getDetails() == null) {
            return Set.of();
        }
        Object raw = measure.getDetails().get(key);
        if (!(raw instanceof Collection<?> collection)) {
            return Set.of();
        }
        Set<String> result = new LinkedHashSet<>();
        for (Object item : collection) {
            if (item == null) {
                continue;
            }
            String value = item.toString().trim();
            if (!value.isEmpty()) {
                result.add(value);
            }
        }
        return result;
    }

    private void updateAssetCollectionLinks(String collectionName,
                                            Set<String> newIds,
                                            Set<String> oldIds,
                                            String measureId) {
        var toAdd = new HashSet<>(newIds);
        toAdd.removeAll(oldIds);
        addMeasureIdToAssets(collectionName, toAdd, measureId);

        var toRemove = new HashSet<>(oldIds);
        toRemove.removeAll(newIds);
        pullMeasureIdFromAssets(collectionName, toRemove, measureId);
    }

    private void addMeasureIdToAssets(String collectionName, Collection<String> assetIds, String measureId) {
        if (assetIds.isEmpty()) {
            return;
        }
        for (String assetId : assetIds) {
            var updateResult = mongoTemplate.updateFirst(
                    query(where("_id").is(assetId)),
                    new Update().addToSet("measureIds", measureId),
                    collectionName);
            if (updateResult.getMatchedCount() == 0) {
                log.warn("Asset {} not found in collection {} while adding measure link {}", assetId, collectionName, measureId);
            }
        }
    }

    private void pullMeasureIdFromAssets(String collectionName, Collection<String> assetIds, String measureId) {
        if (assetIds.isEmpty()) {
            return;
        }
        for (String assetId : assetIds) {
            mongoTemplate.updateFirst(
                    query(where("_id").is(assetId)),
                    new Update().pull("measureIds", measureId),
                    collectionName);
        }
    }
}
