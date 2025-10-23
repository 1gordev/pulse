package com.id.pulse.modules.channel.model;

import com.id.pulse.modules.channel.model.enums.PulseAggregationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "PulseChannelGroup")
public class PulseChannelGroupEntity {

    public static final String ID = "id";
    public static final String CODE = "code";
    public static final String DESCRIPTION = "description";
    public static final String CONNECTORS = "connectors";
    public static final String ENABLED = "enabled";

    public static final String INTERVAL = "interval";
    public static final String DEFAULT_VALIDITY = "defaultValidity";

    public static final String PERSIST = "persist";
    public static final String PERSISTED_LIFE_TIME = "persistedLifeTime";

    @Id
    @Field("_id")
    private String id;

    @Indexed(unique = true)
    private String code;

    private String description;
    private List<String> connectors;
    private Boolean enabled;

    private Long interval;
    private Long defaultValidity;

    private Boolean persistEnabled;
    private String persistedLifeTime;
}
