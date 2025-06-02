package com.id.pulse.modules.measures.model;

import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.measures.model.enums.PulseTransformType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "PulseMeasure")
public class PulseMeasureEntity {

    public static final String ID = "ID";
    public static final String PATH = "PATH";
    public static final String UPSTREAMS = "UPSTREAMS";
    public static final String DESCRIPTION = "DESCRIPTION";
    public static final String UNIT_OF_MEASURE_CODE = "UNIT_OF_MEASURE_CODE";
    public static final String DATA_TYPE = "DATA_TYPE";
    public static final String VALIDITY = "VALIDITY";
    public static final String PRECISION = "PRECISION";
    public static final String TRANSFORM_TYPE = "TRANSFORM_TYPE";
    public static final String DETAILS = "DETAILS";

    @Id
    private String id;

    @Indexed(unique = true)
    private String path;

    private List<PulseUpStream> upstreams;

    private String description;
    private String unitOfMeasureCode;

    private PulseDataType dataType;
    private Long validity;
    private Long precision;
    private PulseTransformType transformType;
    private Map<String, Object> details;

}
