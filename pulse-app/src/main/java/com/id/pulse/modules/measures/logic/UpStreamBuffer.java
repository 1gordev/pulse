package com.id.pulse.modules.measures.logic;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.datapoints.extractor.logic.DataExtractorProcessor;
import com.id.pulse.modules.parser.PulseDataMatrixParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class UpStreamBuffer {

    private final ApplicationContext appCtx;
    private final Map<Long, PulseDataPoint> buffer = Collections.synchronizedMap(new TreeMap<>());
    private String path;
    private Instant currentStart;
    private Instant currentEnd;

    public UpStreamBuffer(ApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    public void init(String path, Instant from, Instant to) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be null or empty");
        }
        if (from == null || to == null) {
            throw new IllegalArgumentException("Time range cannot be null");
        }
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("Start time cannot be after end time");
        }

        // Initialize the buffer
        this.path = path;
        fetch(from, to);
    }


    public void refresh(Instant from, Instant to) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("Time range cannot be null");
        }
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("Start time cannot be after end time");
        }

        if (currentEnd != null && to.isAfter(currentEnd)) {
            // Fetch new data
            fetch(currentEnd, to);
        }

        if (currentStart != null && from.isAfter(currentStart)) {
            // Clear old data
            clear(currentStart, from);
        }

        currentStart = from;
        currentEnd = to;
    }

    private void clear(Instant start, Instant end) {
        long startMs = start.toEpochMilli();
        long endMs = end.toEpochMilli();
        buffer.keySet().removeIf(key -> key >= startMs && key < endMs);
    }

    private synchronized void fetch(Instant start, Instant end) {
        var extractor = appCtx.getBean(DataExtractorProcessor.class);

        // Fetch data into a matrix
        var data = extractor.extract(Collections.singletonList(path), start, end);

        // Initialize the buffer with the fetched data or merge it
        synchronized (buffer) {
            for (var dp : PulseDataMatrixParser.from(data).filterByPath(path)) {
                buffer.put(dp.getTms(), dp);
            }
        }
    }
}
