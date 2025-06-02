package com.id.pulse.modules.channel.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseChannelGroup {

    private String id;
    private String code;
    private String description;
    private String connectorCode;
    private Boolean enabled;

    private Long interval;
    private Long defaultValidity;

    private Boolean persistEnabled;
    private String persistedLifeTime;
}
