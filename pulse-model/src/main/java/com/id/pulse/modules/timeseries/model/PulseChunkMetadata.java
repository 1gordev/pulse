package com.id.pulse.modules.timeseries.model;

import com.id.pulse.modules.channel.model.enums.PulseDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseChunkMetadata {

    private String id;
    private String groupCode;
    private String path;
    private String safePath;
    private PulseDataType type;
    private Long samplingRate;
    private String collectionName;

}
