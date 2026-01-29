package com.id.pulse.modules.datapoints.model;

import com.id.pulse.modules.channel.model.enums.PulseDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@CompoundIndex(name = PulseChunk.PATH_START_END_TYPE_IDX, def = "{'path': 1, 'tsStart': 1, 'tsEnd': 1, 'dataType': 1}")
public class PulseChunk<T> {

    public static final String ID = "id";
    public static final String PATH = "path";
    public static final String TS_START = "tsStart";
    public static final String TS_END = "tsEnd";
    public static final String DATA_TYPE = "dataType";
    public static final String TS = "ts";
    public static final String V = "v";
    public static final String BATCH_IDS = "batchIds";

    public static final String PATH_START_END_TYPE_IDX = "path_start_end_type_idx";

    @Id
    private String id;

    private String path;
    private Long tsStart;
    private Long tsEnd;
    private PulseDataType dataType;

    @Builder.Default
    private List<Long> ts = new ArrayList<>();

    @Builder.Default
    private List<T> v = new ArrayList<>();

    @Builder.Default
    private List<String> batchIds = new ArrayList<>();

}
