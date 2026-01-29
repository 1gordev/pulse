package com.id.pulse.modules.replay.repository;

import com.id.pulse.modules.replay.model.entity.ReplayJobEntity;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ReplayJobRepository extends MongoRepository<ReplayJobEntity, String> {
}
