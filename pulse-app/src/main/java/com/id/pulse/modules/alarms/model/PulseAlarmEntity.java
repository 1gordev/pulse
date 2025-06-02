package com.id.pulse.modules.alarms.model;

import com.id.pulse.modules.measures.model.PulseUpStream;
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
@Document(collection = "PulseAlarm")
public class PulseAlarmEntity {

    public static final String ID = "ID";
    public static final String PATH = "PATH";
    public static final String DESCRIPTION = "DESCRIPTION";
    public static final String TARGET_MEASURE_PATH = "TARGET_MEASURE_PATH";
    public static final String ENGAGE_DURATION = "ENGAGE_DURATION";
    public static final String DISENGAGE_DURATION = "DISENGAGE_DURATION";
    public static final String ENGAGE_CONDITION = "ENGAGE_CONDITION";
    public static final String DISENGAGE_CONDITION = "DISENGAGE_CONDITION";
    public static final String DETAILS = "DETAILS";

    @Id
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
