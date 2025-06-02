package com.id.pulse.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PulseTestMeasureTransformRes {

    private Object result;
    @Builder.Default
    private List<String> logOutput = new ArrayList<>();

}
