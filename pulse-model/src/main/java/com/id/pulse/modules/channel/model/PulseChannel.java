package com.id.pulse.modules.channel.model;

import com.id.pulse.modules.channel.model.enums.PulseAggregationType;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseChannel {

    private String id;
    private String path;
    private String channelGroupCode;
    private String sourcePath;

    @Builder.Default
    private PulseDataType dataType = PulseDataType.DOUBLE;
    @Builder.Default
    private Long validity = 0L;
    @Builder.Default
    private Long precision = 3L;
    @Builder.Default
    private PulseAggregationType aggregationType = PulseAggregationType.COPY;
    @Builder.Default
    private Long aggregationTimeBase = 0L;

}
