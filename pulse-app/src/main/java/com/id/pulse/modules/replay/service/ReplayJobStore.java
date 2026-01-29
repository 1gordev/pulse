package com.id.pulse.modules.replay.service;

import com.id.pulse.modules.replay.model.ReplayJob;
import com.id.pulse.modules.replay.model.ReplayJobView;
import com.id.pulse.modules.replay.model.entity.ReplayJobEntity;
import com.id.pulse.modules.replay.repository.ReplayJobRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class ReplayJobStore {

    private final ReplayJobRepository repository;

    public ReplayJobView upsert(ReplayJob job) {
        ReplayJobEntity entity = repository.findById(job.getId())
                .orElseGet(() -> ReplayJobEntity.builder()
                        .id(job.getId())
                        .createdAt(Instant.now())
                        .build());
        if (entity.getCreatedAt() == null) {
            entity.setCreatedAt(Instant.now());
        }

        entity.setConnectorCode(job.getConnectorCode());
        entity.setStatus(job.getStatus());
        entity.setProgress(job.getProgress());
        entity.setStatusMessage(job.getStatusMessage());
        entity.setStartedAt(job.getStartedAt());
        entity.setCompletedAt(job.getCompletedAt());
        entity.setSourceStartTimestamp(job.getSourceStartTimestamp());
        entity.setSourceEndTimestamp(job.getSourceEndTimestamp());
        entity.setBatchId(job.getBatchId());

        ReplayJobEntity saved = repository.save(entity);
        return toView(saved);
    }

    public Optional<ReplayJobView> findById(String id) {
        return repository.findById(id).map(this::toView);
    }

    public List<ReplayJobView> listLatest(int max) {
        return repository.findAll(Sort.by(Sort.Direction.DESC, "createdAt"))
                .stream()
                .limit(max)
                .map(this::toView)
                .toList();
    }

    public void pruneHistory(int max) {
        List<ReplayJobEntity> ordered = repository.findAll(Sort.by(Sort.Direction.DESC, "createdAt"));
        if (ordered.size() <= max) {
            return;
        }
        ordered.subList(max, ordered.size()).forEach(entity -> repository.deleteById(entity.getId()));
    }

    private ReplayJobView toView(ReplayJobEntity entity) {
        return new ReplayJobView(
                entity.getId(),
                entity.getConnectorCode(),
                entity.getStatus(),
                entity.getProgress() == null ? 0 : entity.getProgress(),
                entity.getStatusMessage(),
                entity.getStartedAt(),
                entity.getCompletedAt(),
                entity.getSourceStartTimestamp(),
                entity.getSourceEndTimestamp(),
                entity.getBatchId()
        );
    }
}
