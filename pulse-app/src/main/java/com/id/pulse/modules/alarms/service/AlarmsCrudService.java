package com.id.pulse.modules.alarms.service;

import com.id.pulse.modules.alarms.PulseAlarm;
import com.id.pulse.modules.alarms.model.PulseAlarmEntity;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.channel.service.ChannelsCrudService;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseUpStream;
import com.id.pulse.modules.measures.model.enums.PulseTransformType;
import com.id.pulse.modules.measures.service.MeasuresCrudService;
import com.id.px3.crud.logic.PxDefaultCrudServiceMongo;
import com.id.px3.crud.logic.PxDefaultMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Service
@Slf4j
public class AlarmsCrudService extends PxDefaultCrudServiceMongo<PulseAlarm, PulseAlarmEntity, String> {

    private final ChannelsCrudService channelsCrudService;
    private final MeasuresCrudService measuresCrudService;
    private final MongoTemplate mongoTemplate;

    public AlarmsCrudService(ChannelsCrudService channelsCrudService,
                             MeasuresCrudService measuresCrudService,
                             MongoTemplate mongoTemplate) {
        super(mongoTemplate,
                new PxDefaultMapper<>(PulseAlarm.class, PulseAlarmEntity.class),
                PxDefaultCrudServiceMongo.DEFAULT_COLLECTION_NAME);
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
        measuresCrudService.save(createEngageMeasure(null, alarm));
        return alarm;
    }


    @Override
    @Transactional
    public PulseAlarm update(String id, PulseAlarm model) {
        var alarm = super.update(id, model);
        var targetMeasureId = measuresCrudService.findByPath(alarm.getTargetMeasurePath())
                .orElseThrow(() -> new IllegalArgumentException("Target measure not found for alarm: " + id))
                .getId();
        measuresCrudService.update(targetMeasureId, createEngageMeasure(targetMeasureId, alarm));
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
     * Find all alarms whose details.physicalAssetId list contains the given ID.
     */
    public List<PulseAlarm> findAllReferringPhysicalAsset(String physicalAssetId) {
        // Fetch the raw entity documents
        List<PulseAlarmEntity> entities = mongoTemplate.find(query(where("details.physicalAssetId").is(physicalAssetId)), PulseAlarmEntity.class);

        // Convert to your domain model (assuming you have a converter method)
        return entities.stream()
                .map(getMapper()::toModel)
                .toList();
    }

    private PulseMeasure createEngageMeasure(String targetMeasureId, PulseAlarm alarm) {



        return PulseMeasure.builder()
                .id(targetMeasureId)
                .path(alarm.getTargetMeasurePath())
                .upstreams(alarm.getUpstreams())
                .description("ALARM: " + alarm.getDescription())
                .dataType(PulseDataType.BOOLEAN)
                .transformType(PulseTransformType.COPY_LATEST)
                .details(Map.of(
                        "alarm_id", alarm.getId(),
                        "js_script", createEngageConditionScript(alarm)
                ))
                .build();
    }

    private String createEngageConditionScript(PulseAlarm alarm) {
        // Create blocks to get upstreams values
        List<String> functions = new ArrayList<>();
        List<String> statements = new ArrayList<>();
        alarm.getUpstreams().forEach(ups -> {
            functions.add(createExtractionFunction(ups));
            statements.add(createExtractionStatement(ups));
        });

        // Assemble the script
        return String.join("\n", functions) + "\n" +
               "function evaluate() {\n" +
               "    let vals = {};\n" +
               "    " + String.join("\n", statements) + "\n" +
               "    return " + alarm.getCondition() + ";\n" +
               "}\n" +
               "evaluate();";
    }

    private String createExtractionStatement(PulseUpStream ups) {
        return "vals['%s'] = _extract_%s();".formatted(
                ups.getPath(),
                ups.getPath().replace("/", "_")
        );
    }

    private String createExtractionFunction(PulseUpStream ups) {
        var dataType = switch (ups.getSourceType()) {
            case CHANNEL -> channelsCrudService.findByPath(ups.getPath())
                    .orElseThrow(() -> new IllegalArgumentException("Channel not found: " + ups.getPath()))
                    .getDataType();
            case MEASURE -> measuresCrudService.findByPath(ups.getPath())
                    .orElseThrow(() -> new IllegalArgumentException("Measure not found: " + ups.getPath()))
                    .getDataType();
        };

        return """
                function _extract_%s() {
                    const allVals = parser.to%s("%s", %s);
                    const lastVal = allVals.at(-1);
                    return lastVal;
                }
                """.formatted(
                ups.getPath().replace("/", "_"),
                switch (dataType) {
                    case BOOLEAN -> "Booleans";
                    case LONG, DOUBLE -> "Numbers";
                    case STRING -> "Strings";
                },
                ups.getPath(),
                switch (dataType) {
                    case BOOLEAN -> "true";
                    case LONG, DOUBLE -> "0.0";
                    case STRING -> "\"\"";
                });

    }
}
