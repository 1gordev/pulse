package com.id.pulse.modules.alarms;

import com.id.pulse.modules.measures.model.PulseUpStream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseAlarm {

    private String id;
    private String path;

    @Builder.Default
    private String description = "";

    @Builder.Default
    private String targetMeasurePath = "";

    @Builder.Default
    private Long engageDuration = 0L;

    @Builder.Default
    private Long disengageDuration = 0L;

    @Builder.Default
    private String engageCondition = "";

    @Builder.Default
    private String disengageCondition = "";

    @Builder.Default
    private Map<String, Object> details = new HashMap<>();

}
