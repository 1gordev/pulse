package com.id.pulse.modules.replay.service;

import com.id.pulse.modules.replay.logic.ReplayInjector;
import com.id.pulse.modules.replay.model.ReplayJob;
import com.id.pulse.modules.replay.model.ReplayJobStatus;
import com.id.pulse.modules.replay.model.ReplayJobView;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ReplayService {

    private final ObjectProvider<ReplayInjector> replayInjectorProvider;
    private final ReplayJobStore replayJobStore;
    private final ConcurrentHashMap<String, ReplayJob> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ReplayJob> activeJobsByConnector = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Deque<String> jobOrder = new ConcurrentLinkedDeque<>();

    private static final int MAX_HISTORY = 100;

    public ReplayService(ObjectProvider<ReplayInjector> replayInjectorProvider,
                         ReplayJobStore replayJobStore) {
        this.replayInjectorProvider = replayInjectorProvider;
        this.replayJobStore = replayJobStore;
    }

    public ReplayJobView startReprocessing(String connectorCode) {
        ReplayJob existingActive = activeJobsByConnector.compute(connectorCode, (code, current) -> {
            if (current == null) {
                return null;
            }
            if (isActive(current)) {
                return current;
            }
            return null;
        });
        if (existingActive != null) {
            return existingActive.toView();
        }

        ReplayJob job = new ReplayJob(UUID.randomUUID().toString(), connectorCode);
        job.setStatus(ReplayJobStatus.PENDING);
        registerJob(job);
        activeJobsByConnector.put(connectorCode, job);
        replayJobStore.upsert(job);

        ReplayInjector injector = replayInjectorProvider.getObject();
        CompletableFuture
                .runAsync(() -> injector.reprocess(job), executorService)
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        job.setStatus(ReplayJobStatus.FAILED);
                        job.setStatusMessage(throwable.getMessage());
                        replayJobStore.upsert(job);
                    }
                    if (!isActive(job)) {
                        activeJobsByConnector.compute(connectorCode, (code, current) -> current == job ? null : current);
                    }
                    replayJobStore.pruneHistory(MAX_HISTORY);
                });

        return job.toView();
    }

    public Optional<ReplayJobView> findJob(String jobId) {
        return replayJobStore.findById(jobId);
    }

    public List<ReplayJobView> listJobs() {
        return replayJobStore.listLatest(MAX_HISTORY);
    }

    public Map<String, ReplayJobView> listActiveReplays() {
        Map<String, ReplayJobView> snapshot = new HashMap<>();
        activeJobsByConnector.forEach((connectorCode, job) -> {
            if (isActive(job)) {
                snapshot.put(connectorCode, job.toView());
            }
        });
        return snapshot;
    }

    public ReplayJobView cancelReprocessing(String connectorCode) {
        ReplayJob job = activeJobsByConnector.get(connectorCode);
        if (job == null || !isActive(job)) {
            throw new IllegalArgumentException("No active replay for connector " + connectorCode);
        }
        job.requestCancel();
        job.setStatusMessage("Cancellation requested");
        replayJobStore.upsert(job);
        return job.toView();
    }

    public ReplayJob getJobOrThrow(String jobId) {
        ReplayJob job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalArgumentException("Replay job not found: " + jobId);
        }
        return job;
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdownNow();
    }

    private boolean isActive(ReplayJob job) {
        ReplayJobStatus status = job.getStatus();
        return status == ReplayJobStatus.PENDING || status == ReplayJobStatus.RUNNING;
    }

    private void registerJob(ReplayJob job) {
        jobs.put(job.getId(), job);
        jobOrder.addLast(job.getId());
        pruneHistory();
    }

    private void pruneHistory() {
        int attempts = 0;
        while (jobOrder.size() > MAX_HISTORY && attempts < jobOrder.size()) {
            String oldestId = jobOrder.pollFirst();
            if (oldestId == null) {
                break;
            }
            ReplayJob oldestJob = jobs.get(oldestId);
            if (oldestJob != null && isActive(oldestJob)) {
                jobOrder.addLast(oldestId);
            } else {
                jobs.remove(oldestId);
            }
            attempts++;
        }
        replayJobStore.pruneHistory(MAX_HISTORY);
    }
}
