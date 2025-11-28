package com.id.pulse.modules.measures.model;

import com.id.pulse.model.PulseDataPoint;
import lombok.Getter;

import java.util.List;
import java.util.UUID;


@Getter
public class TransformerRun {

    private final String id = UUID.randomUUID().toString();
    private final List<PulseDataPoint> channelUpStreams;
    private final long tms;
    private final long interval;
    private final boolean reprocessing;
    private final String reprocessingSessionId;

    public TransformerRun(List<PulseDataPoint> channelUpStreams, long tms, long interval) {
        this(channelUpStreams, tms, interval, false, null);
    }

    public TransformerRun(List<PulseDataPoint> channelUpStreams, long tms, long interval, boolean reprocessing, String reprocessingSessionId) {
        this.channelUpStreams = channelUpStreams;
        this.tms = tms;
        this.interval = interval;
        this.reprocessing = reprocessing;
        this.reprocessingSessionId = reprocessingSessionId;
    }
}
