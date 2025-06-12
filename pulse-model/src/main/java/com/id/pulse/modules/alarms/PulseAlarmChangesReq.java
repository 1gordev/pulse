package com.id.pulse.modules.alarms;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PulseAlarmChangesReq {

    @Builder.Default
    private Boolean allPaths = false;

    @Builder.Default
    private List<String> paths = new ArrayList<>();

    @Builder.Default
    private Boolean latestOnly = false;

    @Builder.Default
    private Instant start = Instant.now();

    @Builder.Default
    private Instant end = Instant.now();

}
