package com.id.pulse.modules.alarms.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "PulseAlarmChange")
@CompoundIndex(name = "alarmPath_tms_idx", def = "{'alarmPath': 1, 'tms': 1}")
public class PulseAlarmChangeEntity {

    public static final String ID = "id";
    public static final String ALARM_PATH = "alarmPath";
    public static final String TMS = "tms";
    public static final String DIRECTION = "direction";


    @Id
    private String id;

    @Indexed
    private String alarmPath;

    @Indexed
    private Long tms;

    private Boolean direction;

}
