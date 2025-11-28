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
}
