package com.id.pulse.modules.channel.service;

import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelEntity;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
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
public class ChannelsCrudService extends PxDefaultCrudServiceMongo<PulseChannel, PulseChannelEntity, String> {

    private final MongoTemplate mongoTemplate;

    public ChannelsCrudService(MongoTemplate mongoTemplate) {
        super(mongoTemplate,
                new PxDefaultMapper<>(PulseChannel.class, PulseChannelEntity.class),
                PxDefaultCrudServiceMongo.DEFAULT_COLLECTION_NAME);
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public List<PulseChannel> processAction(String name, Map<String, Object> params) {
        return List.of();
    }


    public List<PulseChannel> findByChannelGroupCode(String code) {
        return mongoTemplate.find(query(where(PulseChannelEntity.CHANNEL_GROUP_CODE).is(code)), getEntityClass(), getCollectionName()).stream()
                .map(getMapper()::toModel)
                .toList();
    }

    public List<PulseChannel> findByPaths(List<String> paths) {
        return mongoTemplate.find(query(where(PulseChannelEntity.PATH).in(paths)), getEntityClass(), getCollectionName()).stream()
                .map(getMapper()::toModel)
                .toList();
    }

    public Optional<PulseChannel> findByPath(String path) {
        return findByPaths(List.of(path)).stream()
                .findFirst();
    }
}
