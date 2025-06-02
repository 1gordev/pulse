package com.id.pulse.modules.measures.model;

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
public class ScriptEvaluatorResult {
    @Builder.Default
    private boolean ok = false;
    @Builder.Default
    private Object result = null;
    @Builder.Default
    private List<String> logOutput = new ArrayList<>();
}
