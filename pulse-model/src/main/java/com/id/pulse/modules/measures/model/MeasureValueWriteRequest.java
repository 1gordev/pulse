package com.id.pulse.modules.measures.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MeasureValueWriteRequest {
    private String path;
    private Double value;
    private Long timestamp;

    /**
     * Optional sampling interval (ms) to be used when persisting the value.
     * When null, downstream falls back to measure validity or default cadence.
     */
    private Long intervalMs;
}
