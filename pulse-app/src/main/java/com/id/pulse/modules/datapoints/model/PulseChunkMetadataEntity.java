package com.id.pulse.modules.datapoints.model;

import com.id.pulse.modules.channel.model.enums.PulseDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "PulseChunkMetadata")
@CompoundIndex(name = PulseChunkMetadataEntity.PATH_TYPE_SAMPLING_RATE_IDX, def = "{'path': 1, 'type': 1, 'samplingRate': 1}")
public class PulseChunkMetadataEntity {

    public static final String ID = "id";
    public static final String GROUP_CODE = "groupCode";
    public static final String PATH = "path";
    public static final String SAFE_PATH = "safePath";
    public static final String TYPE = "type";
    public static final String SAMPLING_RATE = "samplingRate";
    public static final String COLLECTION_NAME = "collectionName";
    public static final String PATH_TYPE_SAMPLING_RATE_IDX = "path_type_samplingRate_idx";

    @Id
    @Field("_id")
    private String id;

    @Indexed
    private String groupCode;

    @Indexed
    private String path;
    private String safePath;
    private PulseDataType type;
    private Long samplingRate;
    private String collectionName;

}
