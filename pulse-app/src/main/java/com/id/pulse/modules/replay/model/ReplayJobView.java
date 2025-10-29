package com.id.pulse.modules.replay.model;

import java.time.Instant;

public record ReplayJobView(
        String id,
        String connectorCode,
        ReplayJobStatus status,
        int progress,
        String statusMessage,
        Instant startedAt,
        Instant completedAt,
        Long sourceStartTimestamp,
        Long sourceEndTimestamp
) {
}
