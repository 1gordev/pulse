package com.id.pulse.modules.measures.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseMeasureRestCompute {

    private String measurePath;

    private Map<String, Object> upstreamValues;

    private Long tms;

    /**
     * Optional sampling interval (ms) of the triggering acquisition group.
     * When provided, downstream writers (e.g., BN outputs) can store results
     * using the same cadence instead of falling back to defaults.
     */
    private Long intervalMs;
}
