package com.id.pulse.modules.connector.runner.csv;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.enums.PulseConnectorStatus;
import com.id.pulse.modules.connector.runner.IPulseConnectorRunner;
import com.id.px3.utils.SafeConvert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class CsvConnectorRunner implements IPulseConnectorRunner {

    private static final int MAX_ROWS_PER_QUERY = 1000; // safety cap per query

    private final ReentrantLock lock = new ReentrantLock();

    // Params/state
    private volatile boolean running = false;
    private String filePath;
    private String timestampColumn = "timestamp";
    private boolean timeRealign = false;
    private double speedMultiplier = 1.0;

    // Streaming state
    private BufferedReader reader;
    private List<String> headers = List.of();
    private Map<String, Integer> colIndex = new HashMap<>();
    private int tsColIdx = -1;

    // Playback state
    private long wallStartTime = 0L;   // System.currentTimeMillis() when opened
    private long fileStartTime = 0L;   // first row original timestamp (ms)
    private String[] nextRowBuffer;    // buffered first unread row tokens
    private boolean eof = false;

    // minor: log suppression for missing columns
    private final Set<String> missingColumnsWarned = new HashSet<>();

    @Override
    public PulseConnectorStatus open(PulseConnector connector) {
        try {
            Objects.requireNonNull(connector, "connector");
            Map<String, Object> params = Optional.ofNullable(connector.getParams()).orElse(Map.of());
            this.filePath = Optional.ofNullable(params.get("filePath")).map(Object::toString).orElse("");
            this.timestampColumn = Optional.ofNullable(params.get("timestampColumn")).map(Object::toString).filter(s -> !s.isBlank()).orElse("timestamp");
            this.timeRealign = SafeConvert.toBoolean(params.getOrDefault("timeRealign", false)).orElse(false);
            this.speedMultiplier = SafeConvert.toDouble(params.getOrDefault("speedMultiplier", 1)).orElse(1.0);
            if (speedMultiplier <= 0) speedMultiplier = 1.0;

            if (filePath == null || filePath.isBlank()) {
                log.error("CSV connector: Missing filePath parameter");
                return PulseConnectorStatus.FAILED;
            }

            File f = new File(filePath);
            if (!f.exists() || !f.isFile()) {
                log.error("CSV connector: File not found: {}", filePath);
                return PulseConnectorStatus.FAILED;
            }

            // Open reader and load header + first row timestamp without loading entire file
            InputStream in = new FileInputStream(f);
            this.reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 64 * 1024);

            // Read header
            String headerLine = reader.readLine();
            if (headerLine == null) {
                log.error("CSV connector: Empty file: {}", filePath);
                safeClose(reader);
                return PulseConnectorStatus.FAILED;
            }
            headerLine = stripBom(headerLine);
            this.headers = parseCsvLine(headerLine);
            if (headers.isEmpty()) {
                log.error("CSV connector: Failed to parse headers in file: {}", filePath);
                safeClose(reader);
                return PulseConnectorStatus.FAILED;
            }
            this.colIndex = new LinkedHashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                colIndex.put(headers.get(i), i);
            }
            this.tsColIdx = colIndex.getOrDefault(timestampColumn, -1);
            if (tsColIdx < 0) {
                log.error("CSV connector: Timestamp column '{}' not found in headers {}", timestampColumn, headers);
                safeClose(reader);
                return PulseConnectorStatus.FAILED;
            }

            // Read first data row to determine fileStartTime; buffer it for later processing
            String firstDataLine;
            while (true) {
                firstDataLine = reader.readLine();
                if (firstDataLine == null) {
                    log.warn("CSV connector: No data rows found in file: {}", filePath);
                    eof = true;
                    break;
                }
                String[] tokens = toArray(parseCsvLine(firstDataLine));
                if (tokens.length == 0) continue;
                Long ts = parseTimestamp(tokens[tsColIdx]);
                if (ts != null && ts > 0) {
                    fileStartTime = ts;
                    nextRowBuffer = tokens; // keep buffered for first query
                    break;
                }
                // skip rows with invalid timestamp
            }

            wallStartTime = System.currentTimeMillis();

            running = true;
            log.info("CSV connector opened. file='{}', tsCol='{}', realign={}, speed={}", filePath, timestampColumn, timeRealign, speedMultiplier);
            return PulseConnectorStatus.CONNECTED;
        } catch (Exception e) {
            log.error("CSV connector: Failed to open", e);
            safeClose(reader);
            reader = null;
            running = false;
            return PulseConnectorStatus.FAILED;
        }
    }

    @Override
    public PulseConnectorStatus close() {
        try {
            running = false;
            safeClose(reader);
            reader = null;
            return PulseConnectorStatus.IDLE;
        } catch (Exception e) {
            log.error("CSV connector: error while closing", e);
            return PulseConnectorStatus.FAILED;
        }
    }

    @Override
    public CompletableFuture<List<PulseDataPoint>> query(Map<PulseChannelGroup, List<PulseChannel>> channelsMap) {
        return CompletableFuture.supplyAsync(() -> {
            if (!running) return List.of();
            lock.lock();
            try {
                if (reader == null || eof) return List.of();

                long now = System.currentTimeMillis();
                long targetOriginalTs;
                if (timeRealign) {
                    long elapsedWall = Math.max(0L, now - wallStartTime);
                    targetOriginalTs = fileStartTime + (long) Math.floor(elapsedWall * speedMultiplier);
                } else {
                    targetOriginalTs = now; // compare with original timestamps as-is
                }

                List<PulseDataPoint> out = new ArrayList<>();
                int emittedRows = 0;

                // loop reading rows up to targetOriginalTs or until safety cap per query
                while (!eof && emittedRows < MAX_ROWS_PER_QUERY) {
                    String[] row = pollNextRow();
                    if (row == null) break; // nothing buffered and cannot read more yet

                    Long rowTs = parseTimestamp(row[tsColIdx]);
                    if (rowTs == null) {
                        // skip rows with invalid ts
                        continue;
                    }
                    if (rowTs > targetOriginalTs) {
                        // Not yet time to emit this row; buffer it back and stop
                        nextRowBuffer = row;
                        break;
                    }

                    long outTms;
                    if (timeRealign) {
                        // Scheduled wall time for this row
                        long dtFile = Math.max(0L, rowTs - fileStartTime);
                        outTms = wallStartTime + (long) Math.floor(dtFile / Math.max(0.0000001, speedMultiplier));
                    } else {
                        outTms = rowTs;
                    }

                    // For each channel requested, pick the proper column from CSV and convert
                    channelsMap.forEach((group, channels) -> {
                        for (PulseChannel ch : channels) {
                            String columnName = Optional.ofNullable(ch.getSourcePath()).filter(s -> !s.isBlank()).orElse(ch.getPath());
                            Integer idx = colIndex.get(columnName);
                            if (idx == null) {
                                if (missingColumnsWarned.add(columnName)) {
                                    log.warn("CSV connector: column '{}' not found for channel '{}' (group '{}')", columnName, ch.getPath(), group.getCode());
                                }
                                return;
                            }
                            String cell = idx < row.length ? row[idx] : null;
                            if (cell == null || cell.isEmpty()) {
                                return;
                            }
                            Optional<?> conv = convertCell(cell, ch.getDataType());
                            conv.ifPresent(val -> out.add(PulseDataPoint.builder()
                                    .groupCode(group.getCode())
                                    .path(ch.getPath())
                                    .tms(outTms)
                                    .type(ch.getDataType())
                                    .val(val)
                                    .build()));
                        }
                    });

                    emittedRows++;
                }

                return out;
            } catch (Exception e) {
                log.error("CSV connector: error during query", e);
                return List.of();
            } finally {
                lock.unlock();
            }
        });
    }

    // Read next row either from buffer or from reader; sets eof if reached end
    private String[] pollNextRow() {
        if (nextRowBuffer != null) {
            String[] out = nextRowBuffer;
            nextRowBuffer = null;
            return out;
        }
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = toArray(parseCsvLine(line));
                if (tokens.length == 0) continue;
                return tokens;
            }
            eof = true;
            return null;
        } catch (IOException e) {
            log.error("CSV connector: failed reading next row", e);
            eof = true;
            return null;
        }
    }

    private static void safeClose(Closeable c) {
        if (c == null) return;
        try { c.close(); } catch (Exception ignored) { }
    }

    private static String stripBom(String s) {
        if (s == null) return null;
        if (!s.isEmpty() && s.charAt(0) == '\uFEFF') {
            return s.substring(1);
        }
        return s;
    }

    // CSV parsing with support for quoted fields and commas inside quotes
    private static List<String> parseCsvLine(String line) {
        if (line == null) return List.of();
        List<String> fields = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // escaped quote
                    cur.append('"');
                    i++; // skip next
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                fields.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        fields.add(cur.toString());
        // trim spaces and surrounding quotes
        for (int i = 0; i < fields.size(); i++) {
            String f = fields.get(i).trim();
            if (f.length() >= 2 && f.startsWith("\"") && f.endsWith("\"")) {
                f = f.substring(1, f.length() - 1);
            }
            fields.set(i, f);
        }
        return fields;
    }

    private static String[] toArray(List<String> list) {
        String[] arr = new String[list.size()];
        for (int i = 0; i < list.size(); i++) arr[i] = list.get(i);
        return arr;
    }

    private static Optional<?> convertCell(String cell, PulseDataType type) {
        try {
            return switch (type) {
                case DOUBLE -> SafeConvert.toDouble(cell).map(v -> v);
                case LONG -> SafeConvert.toLong(cell).map(v -> v);
                case BOOLEAN -> SafeConvert.toBoolean(cell).map(v -> v);
                case STRING -> Optional.of(cell);
            };
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Long parseTimestamp(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.isEmpty()) return null;
        // try numeric
        Optional<Long> asLong = SafeConvert.toLong(s);
        if (asLong.isPresent()) {
            long v = asLong.get();
            if (v < 1_000_000_000_000L && v > 1_000_000_000L) { // very likely seconds
                v = v * 1000L;
            }
            return v;
        }
        // try ISO-8601
        try {
            Instant ins = Instant.parse(s);
            return ins.toEpochMilli();
        } catch (Exception ignored) { }
        // try common patterns (assume system default zone)
        String[] patterns = new String[] {
                "yyyy-MM-dd HH:mm:ss.SSS",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy/MM/dd HH:mm:ss",
                "dd/MM/yyyy HH:mm:ss",
                "MM/dd/yyyy HH:mm:ss"
        };
        for (String p : patterns) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(s, DateTimeFormatter.ofPattern(p));
                return ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception ignored) { }
        }
        return null;
    }
}
