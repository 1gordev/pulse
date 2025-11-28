package com.id.pulse.modules.measures.model;

import com.id.pulse.modules.measures.model.enums.PulseMeasureRegisterHookType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseMeasureRegisterHook {

    private PulseMeasureRegisterHookType type;
    private String audienceId;
    private String postEndPoint;
    private Boolean reprocessing;
    private String reprocessingSessionId;
    /**
     * When reprocessing, carries the original event timestamp (epoch millis) read from the connector source (e.g. CSV).
     * Null for live hooks.
     */
    private Long reprocessingTimestamp;

}
