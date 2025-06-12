package com.id.pulse.modules.alarms.service;

import com.id.pulse.modules.alarms.PulseAlarmChange;
import com.id.pulse.modules.alarms.PulseAlarmChangesReq;
import com.id.pulse.modules.alarms.model.PulseAlarmChangeEntity;
import com.id.px3.crud.logic.PxDefaultCrudServiceMongo;
import com.id.px3.crud.logic.PxDefaultMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class AlarmChangesCrudService extends PxDefaultCrudServiceMongo<PulseAlarmChange, PulseAlarmChangeEntity, String> {

    private final MongoTemplate mongoTemplate;

    public AlarmChangesCrudService(MongoTemplate mongoTemplate) {
        super(mongoTemplate,
                new PxDefaultMapper<>(PulseAlarmChange.class, PulseAlarmChangeEntity.class),
                PxDefaultCrudServiceMongo.DEFAULT_COLLECTION_NAME);
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<PulseAlarmChange> processAction(String name, Map<String, Object> params) {
        return List.of();
    }

    public List<PulseAlarmChange> findFiltered(PulseAlarmChangesReq req) {
        if (!Boolean.TRUE.equals(req.getAllPaths())
            && (req.getPaths() == null || req.getPaths().isEmpty())) {
            return List.of();
        }

        // LATEST-ONLY MODE: group by path and return the latest per path in one aggregation
        if (Boolean.TRUE.equals(req.getLatestOnly())) {
            Aggregation aggregation = buildLatestOnlyAggregation(req);
            AggregationResults<PulseAlarmChangeEntity> results =
                    mongoTemplate.aggregate(aggregation, getCollectionName(), PulseAlarmChangeEntity.class);
            return results.getMappedResults().stream().map(getMapper()::toModel).toList();
        }

        // NORMAL MODE: filter by time window and (optionally) by path(s)
        Query query = new Query();

        // Filter by path(s) unless allPaths = true
        if (!Boolean.TRUE.equals(req.getAllPaths()) && req.getPaths() != null && !req.getPaths().isEmpty()) {
            query.addCriteria(Criteria.where("alarmPath").in(req.getPaths()));
        }

        // Filter by time window
        if (req.getStart() != null && req.getEnd() != null) {
            query.addCriteria(Criteria.where("tms")
                    .gte(req.getStart().toEpochMilli())
                    .lte(req.getEnd().toEpochMilli()));
        }

        return mongoTemplate.find(query, PulseAlarmChangeEntity.class, getCollectionName()).stream().map(getMapper()::toModel).toList();
    }

    private Aggregation buildLatestOnlyAggregation(PulseAlarmChangesReq req) {
        List<AggregationOperation> ops = new ArrayList<>();

        // Optional: Match by paths if needed
        if (!Boolean.TRUE.equals(req.getAllPaths()) && req.getPaths() != null && !req.getPaths().isEmpty()) {
            ops.add(Aggregation.match(Criteria.where("alarmPath").in(req.getPaths())));
        }

        // Sort by tms descending
        ops.add(Aggregation.sort(Sort.by(Sort.Direction.DESC, "tms")));

        // Group by alarmPath, take the first (latest) record in each group
        ops.add(Aggregation.group("alarmPath").first("$$ROOT").as("doc"));

        // Project the grouped doc back to top-level
        ops.add(Aggregation.replaceRoot("doc"));

        return Aggregation.newAggregation(ops);
    }
}
