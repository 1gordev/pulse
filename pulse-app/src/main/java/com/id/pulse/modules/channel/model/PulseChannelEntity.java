package com.id.pulse.modules.channel.model;

import com.id.pulse.modules.channel.model.enums.PulseAggregationType;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "PulseChannel")
public class PulseChannelEntity {

    public static final String ID = "id";
    public static final String PATH = "path";
    public static final String CHANNEL_GROUP_CODE = "channelGroupCode";
    public static final String SOURCE_PATH = "sourcePath";
    public static final String DATA_TYPE = "dataType";
    public static final String VALIDITY = "validity";
    public static final String PRECISION = "precision";
    public static final String AGGREGATION_TYPE = "aggregationType";
    public static final String AGGREGATION_TYPE_BASE = "aggregationTypeBase";

    @Id
    private String id;

    @Indexed(unique = true)
    private String path;

    @Indexed
    private String channelGroupCode;

    @Indexed
    private String sourcePath;

    @Indexed
    private PulseDataType dataType;

    private Long validity;

    private Long precision;

    private PulseAggregationType aggregationType;

    private Long aggregationTimeBase;
}
