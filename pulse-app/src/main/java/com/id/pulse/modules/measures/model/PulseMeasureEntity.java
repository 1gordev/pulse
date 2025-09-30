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
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "PulseMeasure")
public class PulseMeasureEntity {

    public static final String ID = "id";
    public static final String PATH = "path";
    public static final String UPSTREAMS = "upstreams";
    public static final String DESCRIPTION = "description";
    public static final String UNIT_OF_MEASURE_CODE = "unitOfMeasureCode";
    public static final String DATA_TYPE = "dataType";
    public static final String VALIDITY = "validity";
    public static final String PRECISION = "precision";
    public static final String TRANSFORM_TYPE = "transformType";
    public static final String DETAILS = "details";


    @Id
    @Field("_id")
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
