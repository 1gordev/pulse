package com.id.pulse.modules.measures.service;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.model.PulseTestMeasureTransformRes;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.datapoints.ingestor.service.DataIngestor;
import com.id.pulse.modules.measures.model.ScriptEvaluatorResult;
import com.id.pulse.modules.measures.model.TransformerRun;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseUpStream;
import com.id.pulse.modules.measures.model.enums.PulseSourceType;
import com.id.pulse.modules.measures.model.enums.PulseTransformType;
import com.id.pulse.modules.parser.PulseDataMatrixParser;
import com.id.pulse.modules.poller.service.LatestValuesBucket;
import com.id.pulse.modules.timeseries.model.PulseChunkMetadata;
import com.id.pulse.utils.PulseDataMatrixBuilder;
import com.id.px3.utils.SafeConvert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MeasureTransformer {

    private static final int MAX_THREADS = 32;
    public static final String MEASURES_GROUP = "_MEASURES_";
    public static final String DETAILS_ALARM_ID = "alarm_id";

    private final ConcurrentHashMap<String, PulseChunkMetadata> channelMetadata = new ConcurrentHashMap<>();
    private final DataIngestor dataIngestor;
    private final MeasuresCrudService measuresCrudService;
    private final MeasureJsEvaluator measureJsEvaluator;
    private final ExecutorService executor = Executors.newFixedThreadPool(
            MAX_THREADS,
            Thread.ofVirtual().factory()
    );
    private final LatestValuesBucket latestValuesBucket;

    @Autowired
    public MeasureTransformer(MeasuresCrudService measuresCrudService,
                              DataIngestor dataIngestor,
                              MeasureJsEvaluator measureJsEvaluator,
                              LatestValuesBucket latestValuesBucket) {
        this.measuresCrudService = measuresCrudService;
        this.dataIngestor = dataIngestor;
        this.measureJsEvaluator = measureJsEvaluator;
        this.latestValuesBucket = latestValuesBucket;
    }

    public List<PulseDataPoint> execute(TransformerRun run) {
        Map<String, PulseMeasure> measureMap = measuresCrudService.findAll().stream()
                .collect(Collectors.toMap(PulseMeasure::getPath, Function.identity()));

        // TODO: upstream should load valuse considering run.getTms()
        // Get latest values for channels
        Map<String, PulseDataPoint> channelValues = getLatestValues(run.getChannelUpStreams());

        // Build ordered list and get dependency map
        BuildOrderListResult buildOrderListResult = buildOrderedList(measureMap, channelValues.keySet().stream().toList());
        List<PulseMeasure> measures = buildOrderListResult.measures();
        Map<String, Set<String>> origDependencies = buildOrderListResult.origDeps();

        // Load current values for all measures
        var currentValues = measures.stream()
                .map(m -> latestValuesBucket.readAsDataPoint(m.getPath()))
                .filter(Objects::nonNull)
                .toList();

        // Apply transformation logic on ordered measures with dependencies
        List<PulseDataPoint> transformed = applyTransformations(measures, currentValues, origDependencies, channelValues, run.getTms());

        // Persist
        publishToIngestor(transformed, run);

        // Return transformed data points
        return transformed;
    }

    private Map<String, PulseDataPoint> getLatestValues(List<PulseDataPoint> channelUpStreams) {
        // Extract single channel paths
        List<String> paths = channelUpStreams.stream()
                .map(PulseDataPoint::getPath)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        // For each path, find the latest value from the list
        Map<String, PulseDataPoint> latestValues = new HashMap<>();
        for (String path : paths) {
            channelUpStreams.stream()
                    .filter(dp -> dp.getPath().equals(path))
                    .max(Comparator.comparingLong(PulseDataPoint::getTms))
                    .ifPresent(latestValue -> latestValues.put(path, latestValue));
        }

        return latestValues;
    }

    private void publishToIngestor(List<PulseDataPoint> transformed, TransformerRun run) {
        try {
            // Group the transformed data points by channel path
            Map<String, List<PulseDataPoint>> groupedDataPoints = transformed.stream()
                    .collect(Collectors.groupingBy(PulseDataPoint::getPath));

            groupedDataPoints.forEach((path, dataPoints) -> {
                if (path.isEmpty()) {
                    log.error("Empty path for data point");
                    return;
                }
                if (dataPoints.isEmpty()) {
                    return;
                }

                // Get metadata for the channel and write
                var firstDataPoint = dataPoints.getFirst();
                var metadata = channelMetadata.computeIfAbsent("%s:%s".formatted(MEASURES_GROUP, firstDataPoint.getPath()), k ->
                        dataIngestor.prepareMetadata(MEASURES_GROUP, firstDataPoint.getPath(), firstDataPoint.getType(), run.getInterval()));

                // Extract the timeseries from the data points
                Map<Long, Object> timeSeries = new HashMap<>();
                dataPoints.forEach(dataPoint -> {
                    timeSeries.put(dataPoint.getTms(), dataPoint.getVal());
                });

                // Write the data points to the ingestor
                dataIngestor.writeAsync(metadata, timeSeries)
                        .thenAccept(result -> log.trace("Ingestor perfs: %s".formatted(result)))
                        .exceptionally(ex -> {
                            log.error("Error writing data to ingestor", ex);
                            return null;
                        });
            });
        } catch (Exception e) {
            log.error("Error publishing to ingestor: {}", e.getMessage());
        }
    }

    /**
     * Holds the ordered measure list plus the original dependency map.
     */
    public record BuildOrderListResult(
            List<PulseMeasure> measures,
            Map<String, Set<String>> origDeps
    ) {
    }

    public PulseTestMeasureTransformRes testScript(PulseTransformType scriptTransformType,
                                                   String measurePath,
                                                   String script,
                                                   Object currentValue,
                                                   Map<String, Object> testData) {
        if (scriptTransformType != PulseTransformType.JAVASCRIPT) {
            throw new IllegalArgumentException("Only JAVASCRIPT transform type is supported for testing");
        }
        if (testData == null || testData.isEmpty()) {
            throw new IllegalArgumentException("Test data is null or empty");
        }

        // Build measure
        PulseMeasure measure = PulseMeasure.builder()
                .path(measurePath)
                .transformType(scriptTransformType)
                .details(Map.of("js_script", script))
                .build();

        // Build dependencies
        List<PulseDataPoint> deps = new ArrayList<>();
        AtomicLong tms = new AtomicLong(1000L);
        testData.forEach((depPath, depPoints) -> {
            tms.set(1000L);
            if (depPath == null || depPath.isEmpty()) {
                throw new IllegalArgumentException("Dependency path is null or empty");
            }
            if (depPoints == null) {
                throw new IllegalArgumentException("Dependency points are null");
            }

            // depPoints should be a collection of values
            if (depPoints instanceof Collection<?> collection) {
                for (Object point : collection) {
                    deps.add(PulseDataPoint.builder()
                            .groupCode("test")
                            .path(depPath)
                            .tms(tms.get())
                            .type(detectTestPointType(point))
                            .val(point)
                            .build());
                    tms.set(tms.get() + 1000L); // Increment timestamp for each point
                }
            }
        });

        PulseDataPoint currentDataPoint;
        if(currentValue == null){
            var dataType = measuresCrudService.findByPath(measurePath)
                    .map(PulseMeasure::getDataType)
                    .orElse(PulseDataType.DOUBLE);

            currentDataPoint = PulseDataPoint.builder()
                    .groupCode(MEASURES_GROUP)
                    .path(measurePath)
                    .tms(tms.get())
                    .type(dataType)
                    .val(getSafeValue(dataType))
                    .build();
        } else {
            currentDataPoint = PulseDataPoint.builder()
                    .groupCode(MEASURES_GROUP)
                    .path(measurePath)
                    .tms(tms.get())
                    .type(detectTestPointType(currentValue))
                    .val(currentValue)
                    .build();
        }

        AtomicReference<ScriptEvaluatorResult> evaluatorResult = new AtomicReference<>();
        Object scriptResult = transformJavaScript(tms.get(), measure, currentDataPoint, deps, evaluatorResult);

        return PulseTestMeasureTransformRes.builder()
                .result(scriptResult)
                .logOutput(evaluatorResult.get().getLogOutput())
                .build();
    }

    private PulseDataType detectTestPointType(Object point) {
        return switch (point) {
            case String s -> PulseDataType.STRING;
            case Boolean b -> PulseDataType.BOOLEAN;
            case Number number -> {
                if (point instanceof Double) {
                    yield PulseDataType.DOUBLE;
                } else if (point instanceof Long) {
                    yield PulseDataType.LONG;
                } else {
                    throw new IllegalArgumentException("Unsupported number type: " + point.getClass());
                }
            }
            default -> throw new IllegalArgumentException("Unsupported point type: " + point.getClass());
        };
    }

    private BuildOrderListResult buildOrderedList(Map<String, PulseMeasure> measureMap, List<String> channelPaths) {
        // Build inverse graphs
        Map<String, List<String>> channelChildren = new HashMap<>();
        Map<String, List<String>> measureChildren = new HashMap<>();
        for (PulseMeasure m : measureMap.values()) {
            for (PulseUpStream u : m.getUpstreams()) {
                String source = u.getPath();
                if (u.getSourceType() == PulseSourceType.CHANNEL) {
                    channelChildren.computeIfAbsent(source, k -> new ArrayList<>()).add(m.getPath());
                } else {
                    measureChildren.computeIfAbsent(source, k -> new ArrayList<>()).add(m.getPath());
                }
            }
        }

        // Seed impacted set from changed channels
        Set<String> impacted = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        for (String ch : channelPaths) {
            for (String m : channelChildren.getOrDefault(ch, Collections.emptyList())) {
                if (impacted.add(m)) queue.add(m);
            }
        }
        // Propagate through measure dependencies
        while (!queue.isEmpty()) {
            String cur = queue.poll();
            for (String child : measureChildren.getOrDefault(cur, Collections.emptyList())) {
                if (impacted.add(child)) queue.add(child);
            }
        }
        if (impacted.isEmpty()) {
            return new BuildOrderListResult(Collections.emptyList(), Collections.emptyMap());
        }

        // Build dependency map for impacted measures
        Map<String, Set<String>> depsMap = new HashMap<>();
        for (String m : impacted) {
            Set<String> deps = measureMap.get(m).getUpstreams().stream()
                    .filter(u -> u.getSourceType() == PulseSourceType.MEASURE)
                    .map(PulseUpStream::getPath)
                    .filter(impacted::contains)
                    .collect(Collectors.toSet());
            depsMap.put(m, deps);
        }
        // Copy original for later use
        Map<String, Set<String>> origDeps = new HashMap<>();
        depsMap.forEach((k, v) -> origDeps.put(k, new HashSet<>(v)));

        // Topological sort (Kahn's algorithm)
        Deque<String> ready = new ArrayDeque<>();
        for (var entry : depsMap.entrySet()) {
            if (entry.getValue().isEmpty()) ready.add(entry.getKey());
        }
        List<String> order = new ArrayList<>(impacted.size());
        while (!ready.isEmpty()) {
            String node = ready.poll();
            order.add(node);
            for (String child : measureChildren.getOrDefault(node, Collections.emptyList())) {
                if (!depsMap.containsKey(child)) continue;
                Set<String> d = depsMap.get(child);
                if (d.remove(node) && d.isEmpty()) ready.add(child);
            }
        }

        // Cycle-check post-sort by self-reachability
        for (String node : order) {
            if (detectCycle(node, node, origDeps, new HashSet<>())) {
                log.error("Circular dependency detected involving '{}'; skipping all impacted measures", node);
                return new BuildOrderListResult(Collections.emptyList(), origDeps);
            }
        }

        // Map to ordered PulseMeasure list
        List<PulseMeasure> orderedMeasures = order.stream()
                .map(measureMap::get)
                .collect(Collectors.toList());
        return new BuildOrderListResult(orderedMeasures, origDeps);
    }

    private List<PulseDataPoint> applyTransformations(
            List<PulseMeasure> measures,
            List<PulseDataPoint> currentValues,
            Map<String, Set<String>> origDeps,
            Map<String, PulseDataPoint> channelValues,
            long tms) {

        Map<String, CompletableFuture<PulseDataPoint>> futures = new HashMap<>();


        for (PulseMeasure m : measures) {
            // 1) Build futures for measure‐to‐measure deps
            Set<String> measureDeps = origDeps.getOrDefault(m.getPath(), Set.of());
            List<CompletableFuture<PulseDataPoint>> measureFuts = measureDeps.stream()
                    .map(futures::get)
                    .filter(Objects::nonNull)
                    .toList();

            // 2) Immediately pull in any channel deps (they're already completed)
            List<PulseDataPoint> channelDeps = m.getUpstreams().stream()
                    .filter(u -> u.getSourceType() == PulseSourceType.CHANNEL)
                    .map(PulseUpStream::getPath)
                    .map(channelValues::get)
                    .filter(Objects::nonNull)
                    .toList();

            // 3) When all measure‐deps complete, merge with channel‐deps and transform
            CompletableFuture<PulseDataPoint> meFuture = CompletableFuture
                    .allOf(measureFuts.toArray(new CompletableFuture[0]))
                    .thenApplyAsync(ignored -> {
                        // collect measure‐dep results
                        List<PulseDataPoint> resolvedMeasureDeps = measureFuts.stream()
                                .map(CompletableFuture::join)
                                .toList();
                        // merge channel + measure deps
                        List<PulseDataPoint> allDeps = new ArrayList<>(channelDeps.size() + resolvedMeasureDeps.size());
                        allDeps.addAll(channelDeps);
                        allDeps.addAll(resolvedMeasureDeps);

                        // Get current value for this measure
                        PulseDataPoint currentDataPoint;
                        if(currentValues != null) {
                            currentDataPoint = currentValues.stream()
                                    .filter(dp -> dp.getPath().equals(m.getPath()))
                                    .findFirst()
                                    .orElse(PulseDataPoint.builder()
                                            .groupCode(MEASURES_GROUP)
                                            .path(m.getPath())
                                            .tms(tms)
                                            .type(m.getDataType())
                                            .val(getSafeValue(m.getDataType()))
                                            .build());
                        } else {
                            currentDataPoint = PulseDataPoint.builder()
                                    .groupCode(MEASURES_GROUP)
                                    .path(m.getPath())
                                    .tms(tms)
                                    .type(m.getDataType())
                                    .val(getSafeValue(m.getDataType()))
                                    .build();
                        }

                        return transformMeasure(m, currentDataPoint, tms, allDeps);
                    }, executor);

            futures.put(m.getPath(), meFuture);
        }

        // join in original order
        return measures.stream()
                .map(m -> futures.get(m.getPath()).join())
                .toList();


    }

    /**
     * Performs the actual measure transformation. Override this stub
     * to fetch upstream values and apply PulseTransformType logic.
     */
    private PulseDataPoint transformMeasure(PulseMeasure measure, PulseDataPoint currentValue, long tms, List<PulseDataPoint> resolvedDeps) {
        try {
            log.trace("Transforming measure: {}", measure.getPath());

            Object safeVal = getSafeValue(measure.getDataType());
            Object val = safeVal;
            if (!resolvedDeps.isEmpty()) {
                val = switch (measure.getTransformType()) {
                    case COPY_LATEST -> resolvedDeps.getFirst().getVal();
                    case MIN_LATEST -> transformMinLatest(measure.getDataType(), resolvedDeps);
                    case MAX_LATEST -> transformMaxLatest(measure.getDataType(), resolvedDeps);
                    case SUM_LATEST -> transformSumLatest(measure.getDataType(), resolvedDeps);
                    case AVG_LATEST -> transformAvgLatest(measure.getDataType(), resolvedDeps);
                    case JAVASCRIPT -> transformJavaScript(tms, measure, currentValue, resolvedDeps, null);
                };
            } else if (measure.getTransformType() == PulseTransformType.JAVASCRIPT) {
                // Javascript transformations are always applied
                val = transformJavaScript(tms, measure, currentValue, resolvedDeps, null);
            }

            // Cast to the correct type
            if (!val.getClass().equals(safeVal.getClass())) {
                val = castToType(val, safeVal, measure.getDataType());
            }

            // Handle alarm transitions
            if(isAlarm(measure)) {
                handleAlarmTransition(measure, tms, currentValue, val);
            }

            return PulseDataPoint.builder()
                    .groupCode(MEASURES_GROUP)
                    .path(measure.getPath())
                    .tms(tms)
                    .type(measure.getDataType())
                    .val(val)
                    .build();
        } catch (Exception e) {
            log.error("Error transforming measure {}: {}", measure.getPath(), e.getMessage());
            return PulseDataPoint.builder()
                    .groupCode(MEASURES_GROUP)
                    .path(measure.getPath())
                    .tms(tms)
                    .type(measure.getDataType())
                    .val(getSafeValue(measure.getDataType()))
                    .build();
        }
    }

    private void handleAlarmTransition(PulseMeasure measure, long tms, PulseDataPoint currentValue, Object val) {
        if(currentValue.getVal() instanceof Boolean && val instanceof Boolean) {
            if(!currentValue.getVal().equals(val)) {
                // Get alarm ID from measure details
                String alarmId = measure.getDetails().get(DETAILS_ALARM_ID).toString();
                if(alarmId != null && !alarmId.isBlank()) {
                    //
                }
            }
        }
    }

    private boolean isAlarm(PulseMeasure measure) {
        return measure.getDetails().containsKey(DETAILS_ALARM_ID);
    }

    private Object castToType(Object val, Object safeVal, PulseDataType dataType) {
        return switch (dataType) {
            case DOUBLE -> SafeConvert
                    .toDouble(val)
                    .orElse((Double) safeVal);
            case LONG -> SafeConvert
                    .toLong(val)
                    .orElse((Long) safeVal);
            case BOOLEAN -> SafeConvert
                    .toBoolean(val)
                    .orElse((Boolean) safeVal);
            case STRING -> SafeConvert
                    .toString(val)
                    .orElse((String) safeVal);
        };
    }


    private Object getSafeValue(PulseDataType dataType) {
        return switch (dataType) {
            case DOUBLE -> 0d;
            case LONG -> 0L;
            case BOOLEAN -> false;
            case STRING -> "";
        };
    }

    private Object transformMinLatest(PulseDataType dataType, List<PulseDataPoint> deps) {
        return switch (dataType) {
            case DOUBLE -> deps.stream()
                    .mapToDouble(dp -> ((Number) dp.getVal()).doubleValue())
                    .min()
                    .orElse(0.0);
            case LONG -> deps.stream()
                    .mapToLong(dp -> ((Number) dp.getVal()).longValue())
                    .min()
                    .orElse(0L);
            case BOOLEAN -> deps.stream()
                    .map(dp -> (Boolean) dp.getVal())
                    .reduce(Boolean::logicalAnd)
                    .orElse(false);
            case STRING -> deps.stream()
                    .map(dp -> dp.getVal().toString())
                    .min(String::compareTo)
                    .orElse("");
        };
    }

    private Object transformMaxLatest(PulseDataType dataType, List<PulseDataPoint> deps) {
        return switch (dataType) {
            case DOUBLE -> deps.stream()
                    .mapToDouble(dp -> ((Number) dp.getVal()).doubleValue())
                    .max()
                    .orElse(0.0);
            case LONG -> deps.stream()
                    .mapToLong(dp -> ((Number) dp.getVal()).longValue())
                    .max()
                    .orElse(0L);
            case BOOLEAN -> deps.stream()
                    .map(dp -> (Boolean) dp.getVal())
                    .reduce(Boolean::logicalOr)
                    .orElse(false);
            case STRING -> deps.stream()
                    .map(dp -> dp.getVal().toString())
                    .max(String::compareTo)
                    .orElse("");
        };
    }

    private Object transformSumLatest(PulseDataType dataType, List<PulseDataPoint> deps) {
        return switch (dataType) {
            case DOUBLE -> deps.stream()
                    .mapToDouble(dp -> ((Number) dp.getVal()).doubleValue())
                    .sum();
            case LONG -> deps.stream()
                    .mapToLong(dp -> ((Number) dp.getVal()).longValue())
                    .sum();
            default -> throw new UnsupportedOperationException("SUM_LATEST not supported for " + dataType);
        };
    }

    private Object transformAvgLatest(PulseDataType dataType, List<PulseDataPoint> deps) {
        return switch (dataType) {
            case DOUBLE -> deps.stream()
                    .mapToDouble(dp -> ((Number) dp.getVal()).doubleValue())
                    .average()
                    .orElse(0.0);
            case LONG -> (long) deps.stream()
                    .mapToLong(dp -> ((Number) dp.getVal()).longValue())
                    .average()
                    .orElse(0.0);
            default -> throw new UnsupportedOperationException("AVG_LATEST not supported for " + dataType);
        };
    }

    private Object transformJavaScript(Long tsEval,
                                       PulseMeasure m,
                                       PulseDataPoint currentValue,
                                       List<PulseDataPoint> deps,
                                       AtomicReference<ScriptEvaluatorResult> rawResult) {

        // Extract script from measure details
        String script = m.getDetails() != null && m.getDetails().containsKey("js_script")
                ? m.getDetails().get("js_script").toString()
                : null;

        if (script == null) {
            log.error("No JavaScript script found for measure {}", m.getPath());
            return getSafeValue(m.getDataType());
        }

        try {
            // Build a matrix from deps
            var dataMatrix = new PulseDataMatrixBuilder().addSparsePoints(deps).build();

            // Evaluate
            var jsResult = measureJsEvaluator.evaluate(tsEval,
                    script,
                    PulseDataMatrixParser.from(dataMatrix),
                    currentValue.getVal(),
                    "Measure '%s'".formatted(m.getPath()));
            if (rawResult != null) {
                rawResult.set(jsResult);
            }
            return jsResult.getResult();
        } catch (Exception e) {
            log.error("Error transforming measure {}: {}", m.getPath(), e.getMessage());
        }

        return getSafeValue(m.getDataType());
    }

    private boolean detectCycle(String start, String current,
                                Map<String, Set<String>> graph,
                                Set<String> visited) {
        if (!visited.add(current)) return false;
        for (String next : graph.getOrDefault(current, Collections.emptySet())) {
            if (next.equals(start) || detectCycle(start, next, graph, visited)) {
                return true;
            }
        }
        return false;
    }
}
