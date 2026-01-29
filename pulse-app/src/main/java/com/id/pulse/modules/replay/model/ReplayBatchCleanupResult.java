package com.id.pulse.modules.replay.model;

public record ReplayBatchCleanupResult(int pointsRemoved, int docsUpdated, int docsDeleted) {
}
