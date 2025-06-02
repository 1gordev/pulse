package com.id.pulse.modules.connector.service;

import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.PulseConnectorEntity;
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
public class ConnectorsCrudService extends PxDefaultCrudServiceMongo<PulseConnector, PulseConnectorEntity, String> {

    private final MongoTemplate mongoTemplate;

    public ConnectorsCrudService(MongoTemplate mongoTemplate) {
        super(mongoTemplate,
                new PxDefaultMapper<>(PulseConnector.class, PulseConnectorEntity.class),
                PxDefaultCrudServiceMongo.DEFAULT_COLLECTION_NAME);
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<PulseConnector> processAction(String name, Map<String, Object> params) {
        return List.of();
    }

    public Optional<PulseConnector> findByCode(String code) {
        var entity = mongoTemplate.findOne(query(where(PulseConnectorEntity.CODE).is(code)), getEntityClass(), getCollectionName());
        return Optional.ofNullable((entity == null) ? null : getMapper().toModel(entity));
    }
}

