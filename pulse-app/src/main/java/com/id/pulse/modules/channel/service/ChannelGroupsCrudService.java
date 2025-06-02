package com.id.pulse.modules.channel.service;

import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.channel.model.PulseChannelGroupEntity;
import com.id.px3.crud.logic.PxDefaultCrudServiceMongo;
import com.id.px3.crud.logic.PxDefaultMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Service
@Slf4j
public class ChannelGroupsCrudService extends PxDefaultCrudServiceMongo<PulseChannelGroup, PulseChannelGroupEntity, String> {

    private final MongoTemplate mongoTemplate;

    public ChannelGroupsCrudService(MongoTemplate mongoTemplate) {
        super(mongoTemplate,
                new PxDefaultMapper<>(PulseChannelGroup.class, PulseChannelGroupEntity.class),
                PxDefaultCrudServiceMongo.DEFAULT_COLLECTION_NAME);
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<PulseChannelGroup> processAction(String name, Map<String, Object> params) {
        return List.of();
    }

    public Optional<PulseChannelGroup> findByCode(String code) {
        var entity = mongoTemplate.findOne(query(where(PulseChannelGroupEntity.CODE).is(code)), getEntityClass(), getCollectionName());
        return Optional.ofNullable(entity == null ? null : getMapper().toModel(entity));
    }

    public List<PulseChannelGroup> findByCodes(List<String> codes) {
        var mapper = getMapper();
        return mongoTemplate.find(query(where(PulseChannelGroupEntity.CODE).in(codes)), getEntityClass(), getCollectionName()).stream().map(mapper::toModel).toList();
    }
}
