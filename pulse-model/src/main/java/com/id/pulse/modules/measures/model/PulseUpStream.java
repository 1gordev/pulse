package com.id.pulse.modules.measures.model;

import com.id.pulse.modules.measures.model.enums.PulseSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseUpStream {

    private String path;

    @Builder.Default
    private PulseSourceType sourceType = PulseSourceType.CHANNEL;

    @Builder.Default
    private Long bufferLen = 0L;

}
