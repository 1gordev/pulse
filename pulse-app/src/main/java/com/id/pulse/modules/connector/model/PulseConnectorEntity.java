package com.id.pulse.modules.connector.model;

import com.id.pulse.modules.connector.model.enums.PulseConnectorType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "PulseConnector")
public class PulseConnectorEntity {

    public static final String ID = "id";
    public static final String CODE = "code";
    public static final String DESCRIPTION = "description";
    public static final String TYPE = "type";
    public static final String PARAMS = "params";
    public static final String NODE_CODE = "nodeCode";

    @Id
    private String id;

    @Indexed(unique = true)
    private String code;

    private String description;
    private PulseConnectorType type;
    private Map<String, Object> params;
    private String nodeCode;

}
