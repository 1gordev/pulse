package com.id.pulse.modules.poller.service;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.measures.service.MeasureTransformer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class LatestValuesBucket {

    private ConcurrentHashMap<String, PulseDataPoint> latest = new ConcurrentHashMap<>();

    public PulseDataMatrix readAsMatrix() {
        var matrixBuilder = PulseDataMatrix.builder();

        latest.forEach((key, dataPoint) -> {
            var groupCode = key.split(":")[0];
            var path = key.split(":")[1];
            if (groupCode.isBlank() || path.isBlank()) {
                log.warn("Group code or path is blank");
                return;
            }

            var tms = dataPoint.getTms();
            if (tms == null || tms <= 0) {
                log.warn("Timestamp is null or <= 0");
                return;
            }

            var val = dataPoint.getVal();
            if (val == null) {
                log.warn("Value is null");
                return;
            }

            matrixBuilder.add(groupCode, path, tms, val);
        });

        return matrixBuilder.build();
    }

    public CompletableFuture<Void> writeDataSetAsync(List<PulseChannelGroup> groups, List<PulseDataPoint> dataSet) {
        return CompletableFuture.runAsync(() -> writeDataSet(groups, dataSet));
    }

    public void writeDataSet(List<PulseChannelGroup> groups, List<PulseDataPoint> dataPoints) {
        dataPoints.forEach(dataPoint -> writeDataPoint(groups, dataPoint));
    }

    public void writeDataPoint(List<PulseChannelGroup> groups, PulseDataPoint dataPoint) {
        if (groups == null || dataPoint == null) {
            log.warn("Groups or data point is null");
            return;
        }
        if (groups.isEmpty()) {
            log.warn("Groups are empty");
            return;
        }
        if (dataPoint.getGroupCode() == null) {
            log.warn("Group code is null");
            return;
        }

        // Find the group for the data point - Deal with measures as a special case
        var group = (groups.size() == 1 && dataPoint.getGroupCode().equals(MeasureTransformer.MEASURES_GROUP))
                ? groups.getFirst()
                : groups.stream()
                .filter(g -> g.getCode().equals(dataPoint.getGroupCode()))
                .findFirst()
                .orElse(null);
        if (group == null) {
            log.warn("Group not found for code: {}", dataPoint.getGroupCode());
            return;
        }

        if (dataPoint.getPath() == null || dataPoint.getPath().isBlank()) {
            log.warn("Path is null or blank");
            return;
        }
        if (dataPoint.getVal() == null) {
            log.warn("Value is null");
            return;
        }
        if (dataPoint.getType() == null) {
            log.warn("Type is null");
            return;
        }
        if (dataPoint.getTms() == null || dataPoint.getTms() <= 0) {
            log.warn("Timestamp is null or <= 0");
            return;
        }

        // Normalize raw timestamp using group interval
        long ts = dataPoint.getTms() - (dataPoint.getTms() % group.getInterval());
        dataPoint.setTms(ts);

        // Write the value to the bucket
        latest.put("%s:%s".formatted(dataPoint.getGroupCode(), dataPoint.getPath()), dataPoint);
    }

    public PulseDataPoint readAsDataPoint(String path) {
        return latest.get(path);
    }
}
