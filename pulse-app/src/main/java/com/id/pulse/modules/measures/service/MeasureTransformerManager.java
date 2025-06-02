package com.id.pulse.modules.measures.service;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.model.PulseTestMeasureTransformRes;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.measures.model.TransformerRun;
import com.id.pulse.modules.measures.model.enums.PulseTransformType;
import com.id.pulse.modules.poller.service.LatestValuesBucket;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class MeasureTransformerManager {

    private final ApplicationContext appCtx;
    private final Map<String, Long> lastGroupTms = new HashMap<>();
    private final LatestValuesBucket latestValuesBucket;
    private final MeasuresCrudService measuresCrudService;

    public MeasureTransformerManager(ApplicationContext appCtx, LatestValuesBucket latestValuesBucket, MeasuresCrudService measuresCrudService) {
        this.appCtx = appCtx;
        this.latestValuesBucket = latestValuesBucket;
        this.measuresCrudService = measuresCrudService;
    }

    public void run(PulseChannelGroup group, List<PulseDataPoint> dps) {
        if (group == null) {
            log.error("Group is null");
            return;
        }

        if (dps == null) {
            log.error("Data points are null or empty");
            return;
        }

        if (dps.isEmpty()) {
            log.trace("Data points are empty for gorup {}", group.getCode());
            return;
        }

        // Get the timestamp for the run
        long tms = dps.stream().mapToLong(PulseDataPoint::getTms).max().orElseThrow(
                () -> new IllegalStateException("No data points found for the group " + group.getCode())
        );

        // Check if the group has already been run with this timestamp
        if (lastGroupTms.containsKey(group.getCode()) && lastGroupTms.get(group.getCode()).equals(tms)) {
            log.trace("Group {} has already been run with timestamp {}", group.getCode(), tms);
            return;
        }

        // Update the last run timestamp for the group
        lastGroupTms.put(group.getCode(), tms);

        // Create the run metadata object
        var metadata = new TransformerRun(
                dps,
                tms,
                group.getInterval()
        );

        try {
            // Run the transformer
            var transformer = appCtx.getBean(MeasureTransformer.class);
            var result = transformer.execute(metadata);

            // Push results to the latest values bucket
            latestValuesBucket.writeDataSetAsync(List.of(group), result);

        } catch (Exception e) {
            log.error("Error running transformer for group {}: {}", group.getCode(), e.getMessage());
            throw new RuntimeException("Error running transformer", e);
        }
    }

    public PulseTestMeasureTransformRes testTransform(String measurePath, String script, Object currentValue, Map<String, Object> testData) {
        return appCtx.getBean(MeasureTransformer.class).testScript(PulseTransformType.JAVASCRIPT, measurePath, script, currentValue, testData);
    }
}
