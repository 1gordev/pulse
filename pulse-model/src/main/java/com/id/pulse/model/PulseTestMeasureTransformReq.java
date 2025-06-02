package com.id.pulse.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PulseTestMeasureTransformReq {

    private String measurePath = "";
    private String script = "";
    private Object currentValue = null;
    private Map<String, Object> testData = new HashMap<>();

}
