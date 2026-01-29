package com.id.pulse.modules.replay.logic;

import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.channel.service.ChannelGroupsCrudService;
import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.enums.ConnectorCallReason;
import com.id.pulse.modules.connector.model.enums.CsvTimestampFormat;
import com.id.pulse.modules.connector.model.enums.PulseConnectorStatus;
import com.id.pulse.modules.connector.model.enums.PulseConnectorType;
import com.id.pulse.modules.connector.service.ConnectorsCrudService;
import com.id.pulse.modules.connector.service.ConnectionManager;
import com.id.pulse.modules.connector.util.CsvTimestampParser;
import com.id.pulse.modules.orchestrator.service.ConnectorsRegistry;
import com.id.pulse.modules.poller.service.ChannelPoller;
import com.id.pulse.modules.replay.model.ReplayJob;
import com.id.pulse.modules.replay.model.ReplayJobStatus;
import com.id.pulse.modules.replay.model.ReprocessingSessionStatus;
import com.id.pulse.modules.replay.service.ReprocessingStatusNotifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class ReplayInjector {

    private static final String CSV_PROCESSING_MODE_REPROCESSING = "RE_PROCESSING";
    private static final String CSV_PROCESSING_MODE_TIME_REALIGN = "TIME_REALIGN";
    private static final String DEFAULT_TIMESTAMP_COLUMN = "timestamp";
    private static final Duration CONNECTOR_READY_TIMEOUT = Duration.ofSeconds(30);
    private static final long CONNECTOR_STATUS_POLL_MS = 200L;
    private static final long LOOP_IDLE_SLEEP_MS = 50L;
    private static final int LOOP_IDLE_THRESHOLD = 40;
    private final ConnectorsCrudService connectorsCrudService;
    private final ChannelGroupsCrudService channelGroupsCrudService;
    private final ConnectionManager connectionManager;
    private final ConnectorsRegistry connectorsRegistry;
    private final ChannelPoller channelPoller;
    private final ReprocessingStatusNotifier reprocessingStatusNotifier;
    private final com.id.pulse.modules.replay.service.ReplayJobStore replayJobStore;

    public ReplayInjector(ConnectorsCrudService connectorsCrudService,
                          ChannelGroupsCrudService channelGroupsCrudService,
                          ConnectionManager connectionManager,
                          ConnectorsRegistry connectorsRegistry,
                          ChannelPoller channelPoller,
                          ReprocessingStatusNotifier reprocessingStatusNotifier,
                          com.id.pulse.modules.replay.service.ReplayJobStore replayJobStore) {
        this.connectorsCrudService = connectorsCrudService;
        this.channelGroupsCrudService = channelGroupsCrudService;
        this.connectionManager = connectionManager;
        this.connectorsRegistry = connectorsRegistry;
        this.channelPoller = channelPoller;
        this.reprocessingStatusNotifier = reprocessingStatusNotifier;
        this.replayJobStore = replayJobStore;
    }

    public void reprocess(ReplayJob job) {
        log.info("Starting reprocessing for connector {}", job.getConnectorCode());
        try {
            job.setStatus(ReplayJobStatus.RUNNING);
            reprocessingStatusNotifier.notifyStatus(job.getId(), ReprocessingSessionStatus.STARTED);
            replayJobStore.upsert(job);

            PulseConnector connector = connectorsCrudService.findByCode(job.getConnectorCode())
                    .orElseThrow(() -> new IllegalArgumentException("Connector not found: " + job.getConnectorCode()));

            ConnectorCallReason callReason = resolveCallReason(connector);
            validateConnector(connector, callReason);
            CsvBounds bounds = resolveCsvBounds(connector);
            job.setSourceBounds(bounds.startTimestamp(), bounds.endTimestamp());
            replayJobStore.upsert(job);
            job.updateProgress(0);
            replayJobStore.upsert(job);

            List<PulseChannelGroup> groups = channelGroupsCrudService.findByConnectorCode(connector.getCode());
            if (groups.isEmpty()) {
                throw new IllegalStateException("No channel groups linked to connector " + connector.getCode());
            }

            if (!ensureConnectorReady(job, connector.getCode())) {
                job.setStatus(ReplayJobStatus.CANCELLED);
                job.setStatusMessage("Reprocessing cancelled");
                replayJobStore.upsert(job);
                log.info("Reprocessing cancelled before start for connector {}", connector.getCode());
                reprocessingStatusNotifier.notifyStatus(job.getId(), ReprocessingSessionStatus.CANCELLED);
                return;
            }

            connectionManager.setReplayMode(connector.getCode(), true);
            job.setBatchId(connectionManager.getBatchId(connector.getCode()));
            replayJobStore.upsert(job);
            ReprocessLoopResult loopResult = executeReprocessingLoop(job, connector.getCode(), groups, bounds, callReason);

            if (loopResult.cancelled()) {
                job.setStatus(ReplayJobStatus.CANCELLED);
                job.setStatusMessage("Reprocessing cancelled");
                replayJobStore.upsert(job);
                log.info("Reprocessing cancelled for connector {}", connector.getCode());
                reprocessingStatusNotifier.notifyStatus(job.getId(), ReprocessingSessionStatus.CANCELLED);
                return;
            }

            if (!loopResult.producedData()) {
                throw new IllegalStateException("Reprocessing finished without producing data");
            }
            if (!loopResult.reachedEnd()) {
                throw new IllegalStateException("Reprocessing terminated before reaching the end of the dataset");
            }

            job.updateProgress(100);
            job.setStatus(ReplayJobStatus.COMPLETED);
            job.setStatusMessage("Reprocessing completed");
            replayJobStore.upsert(job);
            log.info("Reprocessing completed for connector {}", connector.getCode());
            reprocessingStatusNotifier.notifyStatus(job.getId(), ReprocessingSessionStatus.COMPLETED);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            job.setStatus(ReplayJobStatus.FAILED);
            job.setStatusMessage(e.getMessage());
            replayJobStore.upsert(job);
            log.error("Reprocessing failed for connector {}: {}", job.getConnectorCode(), e.getMessage(), e);
            reprocessingStatusNotifier.notifyStatus(job.getId(), ReprocessingSessionStatus.FAILED);
        } finally {
            try {
                connectionManager.setReplayMode(job.getConnectorCode(), false);
                connectionManager.terminateConnection(job.getConnectorCode());
            } catch (Exception ex) {
                log.warn("Failed terminating connector {} after replay", job.getConnectorCode(), ex);
            }
        }
    }

    private void validateConnector(PulseConnector connector, ConnectorCallReason callReason) {
        if (connector.getType() != PulseConnectorType.CSV) {
            throw new IllegalArgumentException("Connector " + connector.getCode() + " is not CSV");
        }
        if (callReason == ConnectorCallReason.TIME_REALIGN) {
            throw new IllegalArgumentException("Connector " + connector.getCode() + " is configured for TIME_REALIGN, which is not supported yet");
        }
    }

    private ConnectorCallReason resolveCallReason(PulseConnector connector) {
        Map<String, Object> params = Optional.ofNullable(connector.getParams()).orElse(Map.of());
        String processingMode = Optional.ofNullable(params.get("processingMode"))
                .map(Object::toString)
                .map(mode -> mode.toUpperCase(Locale.ROOT))
                .orElse(CSV_PROCESSING_MODE_REPROCESSING);
        return switch (processingMode) {
            case CSV_PROCESSING_MODE_REPROCESSING -> ConnectorCallReason.RE_PROCESSING;
            case CSV_PROCESSING_MODE_TIME_REALIGN -> ConnectorCallReason.TIME_REALIGN;
            default -> throw new IllegalArgumentException("Unsupported processing mode '%s' for connector %s"
                    .formatted(processingMode, connector.getCode()));
        };
    }

    private CsvBounds resolveCsvBounds(PulseConnector connector) throws IOException {
        Map<String, Object> params = Optional.ofNullable(connector.getParams()).orElse(Map.of());
        String filePath = Optional.ofNullable(params.get("filePath"))
                .map(Object::toString)
                .filter(path -> !path.isBlank())
                .orElseThrow(() -> new IllegalArgumentException("Connector " + connector.getCode() + " has no filePath"));

        String timestampColumn = Optional.ofNullable(params.get("timestampColumn"))
                .map(Object::toString)
                .map(String::trim)
                .orElse(DEFAULT_TIMESTAMP_COLUMN);
        CsvTimestampFormat timestampFormat = CsvTimestampParser.resolveFormat(params.get("timestampFormat"));
        int timestampOffsetMinutes = CsvTimestampParser.resolveOffsetMinutes(params.get("timestampOffset"));
        int headerRowIndex = normalizeIndex(params.get("headerRowIndex"), 0);
        int dataRowIndex = normalizeIndex(params.get("dataRowIndex"), headerRowIndex + 1);
        if (dataRowIndex <= headerRowIndex) {
            dataRowIndex = headerRowIndex + 1;
        }
        boolean columnSeparatorProvided = params.get("columnSeparator") != null
                && !params.get("columnSeparator").toString().trim().isEmpty();

        Path csvPath = Path.of(filePath);
        if (!Files.exists(csvPath) || Files.isDirectory(csvPath)) {
            throw new IllegalArgumentException("CSV file not found: " + filePath);
        }

        char columnSeparator = normalizeSeparator(params.get("columnSeparator"), ',');
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            String rawHeader = readLineAtIndex(reader, headerRowIndex);
            if (rawHeader == null) {
                throw new IllegalArgumentException("Header row %d not found in CSV".formatted(headerRowIndex));
            }
            if (!columnSeparatorProvided) {
                columnSeparator = detectSeparator(rawHeader, columnSeparator);
            }
            String headerLine = stripBom(rawHeader);
            if (headerLine == null) {
                throw new IllegalArgumentException("Header row %d not found in CSV".formatted(headerRowIndex));
            }
            CsvHeader header = CsvHeader.parse(stripBom(headerLine), timestampColumn, columnSeparator);
            if (header.timestampIndex() < 0) {
                throw new IllegalArgumentException("Timestamp column '%s' not found in CSV".formatted(timestampColumn));
            }
            skipLines(reader, Math.max(0, dataRowIndex - headerRowIndex - 1));
            long minTs = Long.MAX_VALUE;
            long maxTs = Long.MIN_VALUE;
            long rowCount = 0;

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                List<String> fields = parseCsvLine(line, columnSeparator);
                if (fields.isEmpty()) {
                    continue;
                }
                String timestampValue = header.timestampIndex() < fields.size() ? fields.get(header.timestampIndex()) : null;
                Long ts = CsvTimestampParser.parse(timestampValue, timestampFormat, timestampOffsetMinutes);
                if (ts == null) {
                    continue;
                }
                minTs = Math.min(minTs, ts);
                maxTs = Math.max(maxTs, ts);
                rowCount++;
            }

            if (rowCount == 0 || minTs == Long.MAX_VALUE) {
                throw new IllegalStateException("CSV file contains no valid timestamped rows");
            }

            return new CsvBounds(minTs, maxTs);
        }
    }

    private boolean ensureConnectorReady(ReplayJob job, String connectorCode) throws InterruptedException {
        connectionManager.initiateConnection(connectorCode);
        long start = System.currentTimeMillis();
        PulseConnectorStatus status;
        do {
            status = connectorsRegistry.getStatus(connectorCode);
            if (status == PulseConnectorStatus.CONNECTED) {
                return true;
            }
            if (status == PulseConnectorStatus.FAILED) {
                throw new IllegalStateException("Connector " + connectorCode + " failed to open");
            }
            if (job.isCancellationRequested()) {
                return false;
            }
            Thread.sleep(CONNECTOR_STATUS_POLL_MS);
        } while ((System.currentTimeMillis() - start) < CONNECTOR_READY_TIMEOUT.toMillis());

        throw new IllegalStateException("Timeout waiting for connector " + connectorCode + " to become ready");
    }

    private ReprocessLoopResult executeReprocessingLoop(ReplayJob job,
                                                        String connectorCode,
                                                        List<PulseChannelGroup> groups,
                                                        CsvBounds bounds,
                                                        ConnectorCallReason callReason) throws InterruptedException {
        long latestTimestamp = bounds.startTimestamp();
        int idleIterations = 0;
        boolean producedAtLeastOnce = false;

        while (true) {
            if (connectionManager.isReplayComplete(connectorCode)) {
                updateJobProgress(job, connectorCode, latestTimestamp, bounds);
                return new ReprocessLoopResult(producedAtLeastOnce, true, false);
            }
            if (job.isCancellationRequested()) {
                return new ReprocessLoopResult(producedAtLeastOnce, false, true);
            }
            boolean anyData = false;
            final long[] batchMaxTs = {Long.MIN_VALUE};

            for (PulseChannelGroup group : groups) {
                if (job.isCancellationRequested()) {
                    return new ReprocessLoopResult(producedAtLeastOnce, false, true);
                }
                try {
                    var outcome = channelPoller.replayGroup(group, callReason, job.getId()).join();
                    if (outcome != null && outcome.hasData()) {
                        anyData = true;
                        outcome.latestTimestamp().ifPresent(ts -> {
                            if (ts > batchMaxTs[0]) {
                                batchMaxTs[0] = ts;
                            }
                        });
                    }
                } catch (CompletionException completionException) {
                    Throwable cause = completionException.getCause();
                    if (cause instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    }
                    throw completionException;
                }
            }

            if (anyData) {
                idleIterations = 0;
                producedAtLeastOnce = true;
                if (batchMaxTs[0] != Long.MIN_VALUE) {
                    latestTimestamp = Math.max(latestTimestamp, batchMaxTs[0]);
                }
                updateJobProgress(job, connectorCode, latestTimestamp, bounds);
                if (batchMaxTs[0] != Long.MIN_VALUE && latestTimestamp >= bounds.endTimestamp()) {
                    log.info("Reached end timestamp for connector {}", connectorCode);
                    return new ReprocessLoopResult(true, true, false);
                }
            } else {
                idleIterations++;
                if (idleIterations > LOOP_IDLE_THRESHOLD) {
                    if (connectionManager.isReplayComplete(connectorCode)) {
                        updateJobProgress(job, connectorCode, latestTimestamp, bounds);
                        log.info("Reached EOF for connector {}", connectorCode);
                        return new ReprocessLoopResult(producedAtLeastOnce, true, false);
                    }
                    log.info("No new data after {} idle iterations for connector {}", idleIterations, connectorCode);
                    return new ReprocessLoopResult(producedAtLeastOnce, false, false);
                }
                Thread.sleep(LOOP_IDLE_SLEEP_MS);
            }
        }
    }

    private int computeProgress(long latestTimestamp, CsvBounds bounds) {
        long range = Math.max(1L, bounds.endTimestamp() - bounds.startTimestamp());
        long clamped = Math.max(bounds.startTimestamp(), Math.min(latestTimestamp, bounds.endTimestamp()));
        double ratio = (double) (clamped - bounds.startTimestamp()) / (double) range;
        return (int) Math.round(Math.min(1.0, Math.max(0.0, ratio)) * 100.0);
    }

    private void updateJobProgress(ReplayJob job, String connectorCode, long latestTimestamp, CsvBounds bounds) {
        int runnerPercent = connectionManager.getReplayProgressPercent(connectorCode);
        if (runnerPercent >= 0) {
            job.updateProgress(runnerPercent);
        } else {
            job.updateProgress(computeProgress(latestTimestamp, bounds));
        }
        replayJobStore.upsert(job);
    }

    private static String stripBom(String value) {
        if (value == null) {
            return null;
        }
        if (!value.isEmpty() && value.charAt(0) == '\uFEFF') {
            return value.substring(1);
        }
        return value;
    }

    private static List<String> parseCsvLine(String line, char separator) {
        if (line == null || line.isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == separator && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());

        for (int i = 0; i < result.size(); i++) {
            String value = result.get(i).trim();
            if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
            }
            result.set(i, value);
        }
        return result;
    }

    private char normalizeSeparator(Object raw, char fallback) {
        if (raw == null) {
            return fallback;
        }
        String text = raw.toString();
        if (text.isBlank()) {
            return fallback;
        }
        return text.trim().charAt(0);
    }

    private char detectSeparator(String line, char current) {
        if (line == null) {
            return current;
        }
        int commas = 0;
        int semicolons = 0;
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == ',') {
                    commas++;
                } else if (c == ';') {
                    semicolons++;
                }
            }
        }
        if (semicolons > commas && semicolons > 0) {
            return ';';
        }
        if (commas > semicolons && commas > 0) {
            return ',';
        }
        return current;
    }

    private int normalizeIndex(Object raw, int fallback) {
        if (raw == null) {
            return fallback;
        }
        try {
            int value = Integer.parseInt(raw.toString());
            return Math.max(0, value);
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private String readLineAtIndex(BufferedReader reader, int targetIndex) throws IOException {
        if (targetIndex <= 0) {
            return reader.readLine();
        }
        String line;
        int current = 0;
        while ((line = reader.readLine()) != null) {
            if (current == targetIndex) {
                return line;
            }
            current++;
        }
        return null;
    }

    private void skipLines(BufferedReader reader, int linesToSkip) throws IOException {
        if (linesToSkip <= 0) {
            return;
        }
        for (int i = 0; i < linesToSkip; i++) {
            if (reader.readLine() == null) {
                break;
            }
        }
    }

    private record CsvHeader(List<String> columns, int timestampIndex) {
        static CsvHeader parse(String headerLine, String timestampColumn, char separator) {
            List<String> columns = parseCsvLine(headerLine, separator);
            boolean timestampBlank = timestampColumn == null || timestampColumn.isBlank();
            int idx = -1;
            for (int i = 0; i < columns.size(); i++) {
                String col = columns.get(i);
                String normalized = col == null ? "" : col.trim();
                boolean columnBlank = normalized.isEmpty();
                if (columnBlank) {
                    if (timestampBlank) {
                        if (idx >= 0) {
                            throw new IllegalArgumentException("CSV header contains multiple blank columns; only the timestamp column may be blank.");
                        }
                        idx = i;
                    } else {
                        throw new IllegalArgumentException("CSV header contains blank column names; only the timestamp column may be blank.");
                    }
                    continue;
                }
                if (!timestampBlank && normalized.equalsIgnoreCase(timestampColumn)) {
                    idx = i;
                }
            }
            if (timestampBlank) {
                if (idx < 0) {
                    throw new IllegalArgumentException("Timestamp column configured as blank, but CSV header does not contain a blank column.");
                }
            } else if (idx < 0) {
                throw new IllegalArgumentException("Timestamp column '%s' not found in CSV header %s".formatted(timestampColumn, columns));
            }
            return new CsvHeader(columns, idx);
        }
    }

    private record CsvBounds(long startTimestamp, long endTimestamp) {
    }

    private record ReprocessLoopResult(boolean producedData, boolean reachedEnd, boolean cancelled) {
    }
}
