package com.id.pulse.modules.poller.model;

import java.util.OptionalLong;

public final class PollOutcome {
    private static final PollOutcome EMPTY = new PollOutcome(0, null);

    private final int dataPointCount;
    private final Long latestTimestamp;

    private PollOutcome(int dataPointCount, Long latestTimestamp) {
        this.dataPointCount = dataPointCount;
        this.latestTimestamp = latestTimestamp;
    }

    public static PollOutcome empty() {
        return EMPTY;
    }

    public static PollOutcome withData(int dataPointCount, Long latestTimestamp) {
        return new PollOutcome(dataPointCount, latestTimestamp);
    }

    public boolean hasData() {
        return dataPointCount > 0;
    }

    public int dataPointCount() {
        return dataPointCount;
    }

    public OptionalLong latestTimestamp() {
        return latestTimestamp == null ? OptionalLong.empty() : OptionalLong.of(latestTimestamp);
    }
}