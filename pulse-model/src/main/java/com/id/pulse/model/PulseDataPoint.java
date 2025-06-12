package com.id.pulse.model;

import com.id.pulse.modules.channel.model.enums.PulseDataType;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseDataPoint {

    private String groupCode;
    private String path;
    private Long tms;
    private PulseDataType type;
    private Object val;

}
