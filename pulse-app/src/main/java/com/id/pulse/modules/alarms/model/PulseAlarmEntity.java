package com.id.pulse.modules.alarms.model;

import com.id.pulse.modules.measures.model.PulseUpStream;
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
@Document(collection = "PulseAlarm")
public class PulseAlarmEntity {

    public static final String ID = "id";
    public static final String PATH = "path";
    public static final String DESCRIPTION = "description";
    public static final String TARGET_MEASURE_PATH = "targetMeasurePath";
    public static final String ENGAGE_DURATION = "engageDuration";
    public static final String DISENGAGE_DURATION = "disengageDuration";
    public static final String ENGAGE_CONDITION = "engageCondition";
    public static final String DISENGAGE_CONDITION = "disengageCondition";
    public static final String DETAILS = "details";

    @Id
    @Field("_id")
    private String id;

    @Indexed(unique = true)
    private String path;

    private String description;
    private String targetMeasurePath;
    private Long engageDuration;
    private Long disengageDuration;
    private String engageCondition;
    private String disengageCondition;
    private Map<String, Object> details;

}
