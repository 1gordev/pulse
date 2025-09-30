package com.id.pulse.modules.alarms.service;

import com.id.pulse.modules.alarms.PulseAlarm;
import com.id.pulse.modules.alarms.model.PulseAlarmEntity;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.channel.service.ChannelsCrudService;
import com.id.pulse.modules.measures.logic.AlarmJsCodeGenerator;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseUpStream;
import com.id.pulse.modules.measures.model.enums.PulseSourceType;
import com.id.pulse.modules.measures.model.enums.PulseTransformType;
import com.id.pulse.modules.measures.service.MeasuresCrudService;
import com.id.px3.crud.logic.PxDefaultCrudServiceMongo;
import com.id.px3.crud.logic.PxDefaultMapper;
import lombok.extern.slf4j.Slf4j;
import com.mongodb.client.result.UpdateResult;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Service
@Slf4j
public class AlarmsCrudService extends PxDefaultCrudServiceMongo<PulseAlarm, PulseAlarmEntity, String> {

    private final AlarmJsCodeGenerator alarmJsCodeGenerator;
    private final ChannelsCrudService channelsCrudService;
    private final MeasuresCrudService measuresCrudService;
    private final MongoTemplate mongoTemplate;

    public AlarmsCrudService(AlarmJsCodeGenerator alarmJsCodeGenerator, ChannelsCrudService channelsCrudService,
                             MeasuresCrudService measuresCrudService,
                             MongoTemplate mongoTemplate) {
        super(mongoTemplate,
                new PxDefaultMapper<>(PulseAlarm.class, PulseAlarmEntity.class),
                PxDefaultCrudServiceMongo.DEFAULT_COLLECTION_NAME);
        this.alarmJsCodeGenerator = alarmJsCodeGenerator;
        this.channelsCrudService = channelsCrudService;
        this.measuresCrudService = measuresCrudService;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<PulseAlarm> processAction(String name, Map<String, Object> params) {
        return List.of();
    }

    @Override
    @Transactional
    public PulseAlarm save(PulseAlarm model) {
        var alarm = super.save(model);
        measuresCrudService.save(createTargetMeasure(null, alarm));
        syncAssetLinks(alarm, null);
        return alarm;
    }


    @Override
    @Transactional
    public PulseAlarm update(String id, PulseAlarm model) {
        var previousState = findById(id);
        var alarm = super.update(id, model);
        var targetMeasureId = measuresCrudService.findByPath(alarm.getTargetMeasurePath())
                .orElseThrow(() -> new IllegalArgumentException("Target measure not found for alarm: " + id))
                .getId();
        measuresCrudService.update(targetMeasureId, createTargetMeasure(targetMeasureId, alarm));
        syncAssetLinks(alarm, previousState);
        return alarm;
    }

    @Override
    @Transactional
    public void delete(String id) {
        var alarm = findById(id);
        var targetMeasureId = measuresCrudService.findByPath(alarm.getTargetMeasurePath())
                .orElseThrow(() -> new IllegalArgumentException("Target measure not found for alarm: " + id))
                .getId();
        measuresCrudService.delete(targetMeasureId);
        super.delete(id);
        removeAssetLinks(alarm);
    }

    /**
     * Find all alarms by their paths.
     *
     * @param paths List of alarm paths to search for.
     * @return List of PulseAlarm objects matching the given paths.
     */
    public List<PulseAlarm> findByPaths(List<String> paths) {
        return mongoTemplate.find(query(where(PulseAlarmEntity.PATH).in(paths)), getEntityClass(), getCollectionName()).stream()
                .map(getMapper()::toModel)
                .toList();
    }

    /**
     * Find a alarm by its path.
     *
     * @param path The path of the alarm to find.
     * @return An Optional containing the PulseAlarm if found, or empty if not found.
     */
    public Optional<PulseAlarm> findByPath(String path) {
        return Optional.ofNullable(mongoTemplate.findOne(query(where(PulseAlarmEntity.PATH).is(path)), getEntityClass(), getCollectionName()))
                .map(getMapper()::toModel);
    }

    /**
     * Find all alarms whose details.physicalAssetIds list contains the given ID.
     */
    public List<PulseAlarm> findAllReferringPhysicalAsset(String physicalAssetId) {
        // Fetch the raw entity documents
        List<PulseAlarmEntity> entities = mongoTemplate.find(
                query(where("details.physicalAssetIds").is(physicalAssetId)),
                PulseAlarmEntity.class);

        // Convert to your domain model (assuming you have a converter method)
        return entities.stream()
                .map(getMapper()::toModel)
                .toList();
    }

    private PulseMeasure createTargetMeasure(String targetMeasureId, PulseAlarm alarm) {

        var upstreams = generateUpstreams(alarm);

        return PulseMeasure.builder()
                .id(targetMeasureId)
                .path(alarm.getTargetMeasurePath())
                .upstreams(upstreams)
                .description("ALARM: " + alarm.getDescription())
                .dataType(PulseDataType.BOOLEAN)
                .transformType(PulseTransformType.JAVASCRIPT)
                .details(Map.of(
                        "alarm_id", alarm.getId(),
                        "js_script", generateAlarmConditionScript(alarm, upstreams),
                        "js_script_autogen", true
                ))
                .build();
    }

    private List<PulseUpStream> generateUpstreams(PulseAlarm alarm) {
        var channelOrMeasurePaths = alarmJsCodeGenerator.enumerateReferencedPaths(
                alarm.getEngageCondition(),
                alarm.getDisengageCondition()
        );


        return new ArrayList<>(channelOrMeasurePaths.stream().map(path -> {
            PulseSourceType sourceType;
            if(channelsCrudService.findByPath(path).isPresent()) {
                sourceType = PulseSourceType.CHANNEL;
            } else if (measuresCrudService.findByPath(path).isPresent()) {
                sourceType = PulseSourceType.MEASURE;
            } else {
                throw new IllegalArgumentException("Upstream path not found in channels or measures: " + path);
            }

            return PulseUpStream.builder()
                    .path(path)
                    .sourceType(sourceType)
                    .lookBackMillis(Math.max(alarm.getEngageDuration(), alarm.getDisengageDuration()))
                    .build();
        }).toList());
    }

    private String generateAlarmConditionScript(PulseAlarm alarm, List<PulseUpStream> upStreams) {

        return alarmJsCodeGenerator.generate(
                alarm.getEngageCondition(),
                alarm.getEngageDuration(),
                alarm.getDisengageCondition(),
                alarm.getDisengageDuration(),
                upStreams
        );

    }

    private void syncAssetLinks(PulseAlarm current, PulseAlarm previous) {
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

    private void removeAssetLinks(PulseAlarm alarm) {
        if (alarm == null || alarm.getId() == null || alarm.getId().isBlank()) {
            return;
        }
        var physicalIds = extractIds(alarm, "physicalAssetIds");
        var virtualIds = extractIds(alarm, "virtualAssetIds");
        pullAlarmIdFromAssets("PhysicalAsset", physicalIds, alarm.getId());
        pullAlarmIdFromAssets("VirtualAsset", virtualIds, alarm.getId());
    }

    private Set<String> extractIds(PulseAlarm alarm, String key) {
        if (alarm == null || alarm.getDetails() == null) {
            return Set.of();
        }
        Object raw = alarm.getDetails().get(key);
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
                                            String alarmId) {
        var toAdd = new HashSet<>(newIds);
        toAdd.removeAll(oldIds);
        addAlarmIdToAssets(collectionName, toAdd, alarmId);

        var toRemove = new HashSet<>(oldIds);
        toRemove.removeAll(newIds);
        pullAlarmIdFromAssets(collectionName, toRemove, alarmId);
    }

    private void addAlarmIdToAssets(String collectionName, Collection<String> assetIds, String alarmId) {
        if (assetIds.isEmpty()) {
            return;
        }
        for (String assetId : assetIds) {
            var updateResult = mongoTemplate.updateFirst(
                    query(where("_id").is(assetId)),
                    new Update().addToSet("alarmIds", alarmId),
                    collectionName);
            if (updateResult.getMatchedCount() == 0) {
                log.warn("Asset {} not found in collection {} while adding alarm link {}", assetId, collectionName, alarmId);
            }
        }
    }

    private void pullAlarmIdFromAssets(String collectionName, Collection<String> assetIds, String alarmId) {
        if (assetIds.isEmpty()) {
            return;
        }
        for (String assetId : assetIds) {
            mongoTemplate.updateFirst(
                    query(where("_id").is(assetId)),
                    new Update().pull("alarmIds", alarmId),
                    collectionName);
        }
    }

}
