package com.id.pulse.modules.replay.model.entity;

import com.id.pulse.modules.replay.model.ReplayJobStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(collection = "ReplayJob")
public class ReplayJobEntity {

    @Id
    @Field("_id")
    private String id;

    @Indexed
    private String connectorCode;

    private ReplayJobStatus status;
    private Integer progress;
    private String statusMessage;
    private Instant startedAt;
    private Instant completedAt;
    private Long sourceStartTimestamp;
    private Long sourceEndTimestamp;
    private String batchId;

    @Indexed
    private Instant createdAt;
}
