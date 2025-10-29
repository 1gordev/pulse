package com.id.pulse.modules.replay.model;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ReplayJob {

    private final String id;
    private final String connectorCode;
    private final AtomicReference<ReplayJobStatus> status = new AtomicReference<>(ReplayJobStatus.PENDING);
    private final AtomicInteger progress = new AtomicInteger(0);
    private final AtomicReference<String> statusMessage = new AtomicReference<>("");
    private final AtomicReference<Instant> startedAt = new AtomicReference<>();
    private final AtomicReference<Instant> completedAt = new AtomicReference<>();
    private final AtomicBoolean cancellationRequested = new AtomicBoolean(false);

    private volatile Long sourceStartTimestamp;
    private volatile Long sourceEndTimestamp;

    public ReplayJob(String id, String connectorCode) {
        this.id = Objects.requireNonNull(id, "id");
        this.connectorCode = Objects.requireNonNull(connectorCode, "connectorCode");
    }

    public String getId() {
        return id;
    }

    public String getConnectorCode() {
        return connectorCode;
    }

    public ReplayJobStatus getStatus() {
        return status.get();
    }

    public void setStatus(ReplayJobStatus newStatus) {
        status.set(newStatus);
        if (newStatus == ReplayJobStatus.RUNNING) {
            startedAt.compareAndSet(null, Instant.now());
        }
        if (newStatus == ReplayJobStatus.COMPLETED
                || newStatus == ReplayJobStatus.FAILED
                || newStatus == ReplayJobStatus.CANCELLED) {
            completedAt.compareAndSet(null, Instant.now());
        }
    }

    public int getProgress() {
        return progress.get();
    }

    public void updateProgress(int percent) {
        int clamped = Math.max(0, Math.min(100, percent));
        progress.updateAndGet(current -> Math.max(current, clamped));
    }

    public String getStatusMessage() {
        return statusMessage.get();
    }

    public void setStatusMessage(String message) {
        statusMessage.set(message == null ? "" : message);
    }

    public Instant getStartedAt() {
        return startedAt.get();
    }

    public Instant getCompletedAt() {
        return completedAt.get();
    }

    public void setSourceBounds(Long startTimestamp, Long endTimestamp) {
        this.sourceStartTimestamp = startTimestamp;
        this.sourceEndTimestamp = endTimestamp;
    }

    public Long getSourceStartTimestamp() {
        return sourceStartTimestamp;
    }

    public Long getSourceEndTimestamp() {
        return sourceEndTimestamp;
    }

    public ReplayJobView toView() {
        return new ReplayJobView(
                id,
                connectorCode,
                getStatus(),
                getProgress(),
                getStatusMessage(),
                getStartedAt(),
                getCompletedAt(),
                sourceStartTimestamp,
                sourceEndTimestamp
        );
    }

    public void requestCancel() {
        cancellationRequested.set(true);
    }

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }
}
