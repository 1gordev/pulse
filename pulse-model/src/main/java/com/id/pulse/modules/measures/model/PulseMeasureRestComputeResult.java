package com.id.pulse.modules.measures.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseMeasureRestComputeResult {
    private Object value;
}
