package com.id.pulse.modules.measures.model;

import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.measures.model.enums.PulseTransformType;
import com.id.pulse.modules.measures.model.enums.PulseComputationMode;
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
public class PulseMeasure {

    private String id;
    private String path;

    @Builder.Default
    private List<PulseUpStream> upstreams = new ArrayList<>();

    @Builder.Default
    private String description = "";

    private String unitOfMeasure;

    @Builder.Default
    private PulseDataType dataType = PulseDataType.DOUBLE;
    @Builder.Default
    private Long validity = 0L;
    @Builder.Default
    private Long precision = 3L;
    @Builder.Default
    private PulseTransformType transformType = PulseTransformType.COPY_LATEST;
    @Builder.Default
    private PulseComputationMode realtimeComputationMode = PulseComputationMode.ON_INPUT_TRIGGER;
    @Builder.Default
    private PulseComputationMode reprocessingComputationMode = PulseComputationMode.ON_INPUT_TRIGGER;

    @Builder.Default
    private Map<String, Object> details = new HashMap<>();

}
