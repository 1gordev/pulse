package com.id.pulse.modules.poller.service;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.channel.model.enums.PulseAggregationType;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.channel.service.ChannelGroupsCrudService;
import com.id.pulse.modules.channel.service.ChannelsCrudService;
import com.id.pulse.modules.connector.service.ConnectionManager;
import com.id.pulse.modules.datapoints.ingestor.service.DataIngestor;
import com.id.pulse.modules.datapoints.service.DpAccumulatorsManager;
import com.id.pulse.modules.measures.model.enums.PulseMeasureRegisterHookType;
import com.id.pulse.modules.measures.service.MeasureHookService;
import com.id.pulse.modules.measures.service.MeasureTransformerManager;
import com.id.pulse.modules.orchestrator.service.ChannelGroupsRegistry;
import com.id.pulse.modules.timeseries.model.PulseChunkMetadata;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ChannelPoller {

    private final DpAccumulatorsManager dpAccumulatorsManager;
    private final ChannelsCrudService channelsCrudService;
    private final ChannelGroupsCrudService channelGroupsCrudService;
    private final ChannelGroupsRegistry channelGroupsRegistry;
    private final ConcurrentHashMap<String, Long> scheduledPollTimes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PulseChunkMetadata> channelMetadata = new ConcurrentHashMap<>();
    private final ConnectionManager connectionManager;
    private final DataIngestor dataIngestor;
    private final LatestValuesBucket latestValuesBucket;
    private final MeasureTransformerManager measureTransformerManager;
    private final MeasureHookService measureHookService;

    public ChannelPoller(DpAccumulatorsManager dpAccumulatorsManager,
                         ChannelsCrudService channelsCrudService,
                         ChannelGroupsCrudService channelGroupsCrudService,
                         ChannelGroupsRegistry channelGroupsRegistry,
                         ConnectionManager connectionManager,
                         DataIngestor dataIngestor,
                         LatestValuesBucket latestValuesBucket,
                         MeasureTransformerManager measureTransformerManager,
                         MeasureHookService measureHookService) {
        this.dpAccumulatorsManager = dpAccumulatorsManager;
        this.channelsCrudService = channelsCrudService;
        this.channelGroupsCrudService = channelGroupsCrudService;
        this.channelGroupsRegistry = channelGroupsRegistry;
        this.connectionManager = connectionManager;
        this.dataIngestor = dataIngestor;
        this.latestValuesBucket = latestValuesBucket;
        this.measureTransformerManager = measureTransformerManager;
        this.measureHookService = measureHookService;
    }

    public void run() {
        // Process all running groups
        channelGroupsRegistry.getAllRunning().forEach(this::processGroup);
    }

    private void processGroup(String code) {
        // Load the group
        var group = channelGroupsCrudService.findByCode(code).orElseThrow(() -> new IllegalStateException("Group not found: " + code));

        // Calculate the next poll time according to the group interval
        // Avoid to overwrite the poll time if it is already scheduled
        scheduledPollTimes.computeIfAbsent(code, k -> {
            long ts = System.currentTimeMillis();
            return ts - (ts % group.getInterval());
        });

        // Proceed to actual polling if the time has come
        if (System.currentTimeMillis() > scheduledPollTimes.get(code)) {
            scheduledPollTimes.remove(code);
            pollGroupThenPublish(group);
        }
    }

    private void pollGroupThenPublish(PulseChannelGroup group) {
        // Load all channels
        var channels = channelsCrudService.findByChannelGroupCode(group.getCode());
        if (channels.isEmpty()) {
            return;
        }

        // Build a map from path to channel
        Map<String, PulseChannel> channelMap = channels.stream()
                .collect(Collectors.toMap(PulseChannel::getPath, ch -> ch));

        // Query connector
        connectionManager
                .queryConnector(group.getConnectorCode(), Map.of(group, channels))
                .thenAccept(result -> {
                    // Extract non-aggregated data points
                    var nonAggPaths = channelMap.values().stream()
                            .filter(ch -> ch.getAggregationType() == null
                                          || ch.getAggregationType() == PulseAggregationType.COPY)
                            .map(PulseChannel::getPath)
                            .collect(Collectors.toSet());

                    var nonAggDps = result.stream()
                            .filter(dp -> nonAggPaths.contains(dp.getPath()))
                            .toList();

                    if (!nonAggDps.isEmpty()) {
                        truncateToPrecision(channelMap, nonAggDps);
                        publish(List.of(group), nonAggDps);
                    }

                    // Aggregate and publish
                    var aggDps = aggregate(channels, result);
                    if (!aggDps.isEmpty()) {
                        truncateToPrecision(channelMap, aggDps);
                        publish(List.of(group), aggDps);
                    }
                })
                .exceptionally(ex -> {
                    log.error("Error during queryConnector", ex);
                    return null;
                });
    }

    /**
     * Truncate DOUBLE dataPoints according to the precision defined in channelMap.
     *
     * @param channelMap   lookup from dp.getPath(), PulseChannel
     * @param dataPoints   points to mutate in-place
     */
    private void truncateToPrecision(
            Map<String, PulseChannel> channelMap,
            List<PulseDataPoint> dataPoints
    ) {
        for (PulseDataPoint dp : dataPoints) {
            // lookup channel once
            PulseChannel ch = channelMap.get(dp.getPath());
            if (ch == null || ch.getDataType() != PulseDataType.DOUBLE) {
                continue;
            }
            Long prec = ch.getPrecision();
            if (prec == null || prec < 0) {
                continue;
            }
            Object raw = dp.getVal();
            if (!(raw instanceof Number)) {
                continue;
            }

            double value = ((Number) raw).doubleValue();
            if (prec == 0L) {
                // truncate to integer
                long truncated = (long) value;
                dp.setVal(truncated);
                dp.setType(PulseDataType.LONG);
            } else {
                double factor = Math.pow(10, prec);
                double truncated = Math.floor(value * factor) / factor;
                dp.setVal(truncated);
                dp.setType(PulseDataType.DOUBLE);
            }
        }
    }

    private List<PulseDataPoint> aggregate(List<PulseChannel> channels, List<PulseDataPoint> dataPoints) {
        List<PulseDataPoint> aggregatedPoints = new ArrayList<>();

        // Cycle through aggregation types
        List.of(PulseAggregationType.AVG, PulseAggregationType.MIN, PulseAggregationType.MAX, PulseAggregationType.SUM)
                .forEach(aggType -> {

                    // Select the channels with the aggregation type
                    var aggChannels = channels.stream()
                            .filter(channel -> channel.getAggregationType() == aggType)
                            .toList();

                    aggChannels.forEach(channel -> {
                        // Select datapoints and put them into a treemap ordered by tms
                        var dataPointsOfChannelByTms = dataPoints.stream()
                                .filter(dp -> dp.getPath().equals(channel.getPath()))
                                .collect(Collectors.toMap(PulseDataPoint::getTms, dp -> dp, (a, b) -> a, TreeMap::new));

                        // Accumulate
                        dataPointsOfChannelByTms.values().forEach(dp -> {
                            // Find the lower bound of the accumulator, by normalizing the tms to the aggregation timebase
                            long tmsAccStart = dp.getTms() - (dp.getTms() % channel.getAggregationTimeBase());
                            long tmsAccEnd = tmsAccStart + channel.getAggregationTimeBase();

                            // Push dp into the accumulator
                            var acc = dpAccumulatorsManager.getOrCreate(channel.getChannelGroupCode(), channel.getPath(), tmsAccStart, tmsAccEnd);
                            acc.push(dp);
                        });

                        if(!dataPointsOfChannelByTms.isEmpty()) {
                            // Close accumulators which have been completed
                            var accCompleted = dpAccumulatorsManager.findCompleted(channel.getChannelGroupCode(), channel.getPath(), dataPointsOfChannelByTms.lastKey());

                            // Produce one datapoint for each completed accumulator, according to the selected aggregation type
                            aggregatedPoints.addAll(accCompleted.stream().map(acc -> acc.aggregate(aggType)).toList());

                            // Remove the completed accumulators
                            dpAccumulatorsManager.remove(accCompleted);
                        }
                    });

                });

        return aggregatedPoints;
    }

    private void publish(List<PulseChannelGroup> groups, List<PulseDataPoint> dataPoints) {
        try {
            latestValuesBucket.writeDataSet(groups, dataPoints);
        } catch (Exception e) {
            log.error("Error writing into LatestValuesBucket", e);
            return;
        }

        try {
            publishToIngestor(groups, dataPoints);
        } catch (Exception e) {
            log.error("Error writing to DataIngestor", e);
            return;
        }

        try {
            // Execute pre-transformers hooks
            measureHookService.runHooks(PulseMeasureRegisterHookType.BEFORE_ALL_TRANSFORMERS);
        } catch (Exception e) {
            log.error("Error running measure hooks", e);
            return;
        }

        // Run transformers
        try {
            runMeasureTransformers(groups, dataPoints);
        } catch (Exception e) {
            log.error("Error running measure transformers", e);
            return;
        }

        try {
            // Execute post-transformers hooks
            measureHookService.runHooks(PulseMeasureRegisterHookType.AFTER_ALL_TRANSFORMERS);
        } catch (Exception e) {
            log.error("Error running measure hooks", e);
            return;
        }
    }

    private void runMeasureTransformers(List<PulseChannelGroup> groups, List<PulseDataPoint> dataPoints) {
        groups.forEach(group -> {
            var dps = dataPoints.stream().filter(dp -> dp.getGroupCode().equals(group.getCode())).toList();
            if(dps.isEmpty()) {
                return;
            }

            measureTransformerManager.run(group, dps);
        });
    }

    private void publishToIngestor(List<PulseChannelGroup> groups, List<PulseDataPoint> dataPoints) {
        // Make a groups map
        Map<String, PulseChannelGroup> groupsByCode = groups.stream()
                .collect(Collectors.toMap(PulseChannelGroup::getCode, group -> group));

        // Groups datapoints by path
        Map<String, List<PulseDataPoint>> dataPointsByPath = dataPoints.stream()
                .collect(Collectors.groupingBy(PulseDataPoint::getPath));

        // Write to ingestor
        dataPointsByPath.forEach((path, dataPointsOfPath) -> {
            if (dataPointsOfPath.isEmpty()) {
                return;
            }

            // Get the group for the path
            var firstDataPoint = dataPointsOfPath.getFirst();
            var group = groupsByCode.get(firstDataPoint.getGroupCode());

            // Get metadata for the channel and write
            var metadata = channelMetadata.computeIfAbsent("%s:%s".formatted(group.getCode(), firstDataPoint.getPath()), k ->
                    dataIngestor.prepareMetadata(group.getCode(), firstDataPoint.getPath(), firstDataPoint.getType(), group.getInterval()));

            // Extract the timeseries from the data points
            Map<Long, Object> timeSeries = new HashMap<>();
            dataPointsOfPath.forEach(dataPoint -> {
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
    }
}
