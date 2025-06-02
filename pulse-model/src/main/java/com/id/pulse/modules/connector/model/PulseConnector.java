package com.id.pulse.modules.connector.model;


import com.id.pulse.modules.connector.model.enums.PulseConnectorType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseConnector {

    private String id;
    private String code;
    private String description;

    @Builder.Default
    private PulseConnectorType type = PulseConnectorType.CSV;

    @Builder.Default
    private Map<String, Object> params = new LinkedHashMap<>();

    @Builder.Default
    private String nodeCode = "";

}

