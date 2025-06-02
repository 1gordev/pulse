package com.id.pulse.modules.timeseries.model;

import lombok.*;

import java.time.Duration;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulseIngestorWriteResult {

    @Builder.Default
    private long chunks = 0;
    @Builder.Default
    private long points = 0;
    @Builder.Default
    private Duration duration = Duration.ZERO;

}
