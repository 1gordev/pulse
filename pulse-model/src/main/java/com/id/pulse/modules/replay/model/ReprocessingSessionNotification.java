package com.id.pulse.modules.replay.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReprocessingSessionNotification {
    private String sessionId;
    private ReprocessingSessionStatus status;
}
