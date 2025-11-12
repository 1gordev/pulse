package com.id.pulse.modules.connector.runner.csv;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.enums.CsvTimestampFormat;
import com.id.pulse.modules.connector.model.enums.PulseConnectorStatus;
import com.id.pulse.modules.connector.runner.IPulseConnectorRunner;
import com.id.pulse.modules.connector.util.CsvTimestampParser;
import com.id.px3.utils.SafeConvert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
    private volatile boolean replayMode = false;
    private String filePath = "";
    private String timestampColumn = "timestamp";
    private CsvTimestampFormat timestampFormat = CsvTimestampFormat.ISO_8601;
    private int timestampOffsetMinutes = 0;
    private boolean timeRealign = false;
    private double speedMultiplier = 1.0;
    private char columnSeparator = ',';
    private char decimalSeparator = '.';
    private boolean columnSeparatorProvided = false;
    private boolean decimalSeparatorProvided = false;
    private int headerRowIndex = 0;
    private int dataRowIndex = 1;

    // Streaming state
    private BufferedReader reader;
    private Map<String, Integer> colIndex = new HashMap<>();
    private int tsColIdx = -1;
    private final Map<String, Object> lastChannelValues = new HashMap<>();

    // Playback state
    private long wallStartTime = 0L;   // System.currentTimeMillis() when opened
    private long fileStartTime = 0L;   // first row original timestamp (ms)
    private String[] nextRowBuffer;    // buffered first unread row tokens
    private boolean eof = false;
    private volatile boolean eofReached = false;

    // minor: log suppression for missing columns
    private final Set<String> missingColumnsWarned = new HashSet<>();

    @Override
    public PulseConnectorStatus open(PulseConnector connector) {
        try {
            Objects.requireNonNull(connector, "connector");
            Map<String, Object> params = Optional.ofNullable(connector.getParams()).orElse(Map.of());
            this.filePath = Optional.ofNullable(params.get("filePath")).map(Object::toString).orElse("");
            this.timestampColumn = Optional.ofNullable(params.get("timestampColumn"))
                    .map(Object::toString)
                    .map(String::trim)
                    .orElse("timestamp");
            this.timestampFormat = CsvTimestampParser.resolveFormat(params.get("timestampFormat"));
            this.timestampOffsetMinutes = CsvTimestampParser.resolveOffsetMinutes(params.get("timestampOffset"));
            Object rawColSeparator = params.get("columnSeparator");
            boolean columnSeparatorProvided = rawColSeparator != null && !rawColSeparator.toString().trim().isEmpty();
            this.columnSeparator = columnSeparatorProvided
                    ? normalizeSeparator(rawColSeparator, ',')
                    : ',';
            Object rawDecimalSeparator = params.get("decimalSeparator");
            this.decimalSeparatorProvided = rawDecimalSeparator != null && !rawDecimalSeparator.toString().trim().isEmpty();
            this.decimalSeparator = decimalSeparatorProvided
                    ? normalizeSeparator(rawDecimalSeparator, '.')
                    : '.';
            this.headerRowIndex = SafeConvert.toInteger(params.getOrDefault("headerRowIndex", 0)).orElse(0);
            if (headerRowIndex < 0) {
                headerRowIndex = 0;
            }
            this.dataRowIndex = SafeConvert.toInteger(params.getOrDefault("dataRowIndex", headerRowIndex + 1))
                    .orElse(headerRowIndex + 1);
            if (dataRowIndex <= headerRowIndex) {
                dataRowIndex = headerRowIndex + 1;
            }
            this.timeRealign = SafeConvert.toBoolean(params.getOrDefault("timeRealign", false)).orElse(false);
            this.speedMultiplier = SafeConvert.toDouble(params.getOrDefault("speedMultiplier", 1)).orElse(1.0);
            if (speedMultiplier <= 0) speedMultiplier = 1.0;
            this.lastChannelValues.clear();
            this.eof = false;
            this.eofReached = false;

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

            // Read header at configured row
            String rawHeaderLine = readHeaderLine(reader, headerRowIndex);
            if (rawHeaderLine == null) {
                log.error("CSV connector: Header row {} not found in file: {}", headerRowIndex, filePath);
                safeClose(reader);
                return PulseConnectorStatus.FAILED;
            }
            if (!columnSeparatorProvided) {
                this.columnSeparator = detectSeparator(rawHeaderLine, this.columnSeparator);
            }
            String headerLine = stripBom(rawHeaderLine);
            if (headerLine == null) {
                log.error("CSV connector: Failed to parse header row {}", headerRowIndex);
                safeClose(reader);
                return PulseConnectorStatus.FAILED;
            }
            List<String> headers = parseCsvLine(headerLine, columnSeparator);
            if (headers.isEmpty()) {
                log.error("CSV connector: Failed to parse headers in file: {}", filePath);
                safeClose(reader);
                return PulseConnectorStatus.FAILED;
            }
            this.tsColIdx = locateTimestampColumnIndex(headers);
            this.colIndex = new LinkedHashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                colIndex.put(headers.get(i), i);
            }

            skipLines(reader, Math.max(0, dataRowIndex - headerRowIndex - 1));

            // Read first data row to determine fileStartTime; buffer it for later processing
            String firstDataLine;
            while (true) {
                firstDataLine = reader.readLine();
                if (firstDataLine == null) {
                    log.warn("CSV connector: No data rows found in file: {}", filePath);
                    eof = true;
                    break;
                }
                String[] tokens = toArray(parseCsvLine(firstDataLine, columnSeparator));
                if (tokens.length == 0) continue;
                detectDecimalSeparatorIfNeeded(tokens);
                Long ts = CsvTimestampParser.parse(tokens[tsColIdx], timestampFormat, timestampOffsetMinutes);
                if (ts != null && ts > 0) {
                    fileStartTime = ts;
                    nextRowBuffer = tokens; // keep buffered for first query
                    break;
                }
                // skip rows with invalid timestamp
            }

            wallStartTime = System.currentTimeMillis();

            running = true;
            log.info("CSV connector opened. file='{}', headerRow={}, dataRow={}, tsCol='{}', format={}, offset={}min, sep='{}', dec='{}', realign={}, speed={}",
                    filePath, headerRowIndex, dataRowIndex, timestampColumn, timestampFormat, timestampOffsetMinutes, columnSeparator, decimalSeparator, timeRealign, speedMultiplier);
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
            lastChannelValues.clear();
            return PulseConnectorStatus.IDLE;
        } catch (Exception e) {
            log.error("CSV connector: error while closing", e);
            return PulseConnectorStatus.FAILED;
        }
    }

    @Override
    public void setReplayMode(boolean replayMode) {
        this.replayMode = replayMode;
        if (!replayMode) {
            lastChannelValues.clear();
        }
    }

    @Override
    public CompletableFuture<List<PulseDataPoint>> query(Map<PulseChannelGroup, List<PulseChannel>> channelsMap) {
        return CompletableFuture.supplyAsync(() -> {
            if (!replayMode || !running) {
                return List.of();
            }
            if (!lock.tryLock()) {
                return List.of();
            }
            try {
                if (reader == null) {
                    return List.of();
                }
                List<PulseDataPoint> out = new ArrayList<>();
                int rowsRead = 0;
                while (!eof && rowsRead < MAX_ROWS_PER_QUERY) {
                    String[] row = pollNextRow();
                    if (row == null) {
                        break;
                    }
                    Long rowTs = CsvTimestampParser.parse(row[tsColIdx], timestampFormat, timestampOffsetMinutes);
                    if (rowTs == null) {
                        continue;
                    }
                    rowsRead++;
                    long outTms = computeOutputTimestamp(rowTs);
                    emitRow(channelsMap, row, outTms, out);
                }
                if (rowsRead == 0 && eof) {
                    lastChannelValues.clear();
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
                String[] tokens = toArray(parseCsvLine(line, columnSeparator));
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

    private char normalizeSeparator(Object raw, char fallback) {
        if (raw == null) {
            return fallback;
        }
        String str = raw.toString();
        if (str.isBlank()) {
            return fallback;
        }
        return str.trim().charAt(0);
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

    private String normalizeDecimal(String value) {
        if (value == null) {
            return null;
        }
        if (decimalSeparator == '.' || value.indexOf(decimalSeparator) < 0) {
            return value;
        }
        return value.replace(decimalSeparator, '.');
    }

    private String readHeaderLine(BufferedReader reader, int headerRowIdx) throws IOException {
        if (headerRowIdx <= 0) {
            return reader.readLine();
        }
        String line;
        int current = 0;
        while ((line = reader.readLine()) != null) {
            if (current == headerRowIdx) {
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

    private void detectDecimalSeparatorIfNeeded(String[] tokens) {
        if (decimalSeparatorProvided) {
            return;
        }
        for (String token : tokens) {
            if (token == null) {
                continue;
            }
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (looksLikeDecimal(trimmed, ',')) {
                decimalSeparator = ',';
                decimalSeparatorProvided = true;
                return;
            }
            if (looksLikeDecimal(trimmed, '.')) {
                decimalSeparator = '.';
                decimalSeparatorProvided = true;
                return;
            }
        }
    }

    private boolean looksLikeDecimal(String value, char separator) {
        int idx = value.indexOf(separator);
        if (idx <= 0 || idx == value.length() - 1) {
            return false;
        }
        boolean hasDigitBefore = false;
        boolean hasDigitAfter = false;
        for (int i = 0; i < idx; i++) {
            if (Character.isDigit(value.charAt(i))) {
                hasDigitBefore = true;
                break;
            }
        }
        for (int i = idx + 1; i < value.length(); i++) {
            if (Character.isDigit(value.charAt(i))) {
                hasDigitAfter = true;
                break;
            }
        }
        return hasDigitBefore && hasDigitAfter;
    }

    private int locateTimestampColumnIndex(List<String> headers) {
        boolean timestampBlank = timestampColumn == null || timestampColumn.isBlank();
        int idx = -1;
        for (int i = 0; i < headers.size(); i++) {
            String col = headers.get(i);
            String normalized = col == null ? "" : col.trim();
            boolean columnBlank = normalized.isEmpty();
            if (columnBlank) {
                if (timestampBlank) {
                    if (idx >= 0) {
                        throw new IllegalArgumentException("CSV connector: multiple blank column headers found. Only the timestamp column may be blank.");
                    }
                    idx = i;
                } else {
                    throw new IllegalArgumentException("CSV connector: column headers cannot be blank (only the timestamp column may be blank).");
                }
                continue;
            }
            if (!timestampBlank && normalized.equalsIgnoreCase(timestampColumn)) {
                idx = i;
            }
        }
        if (timestampBlank && idx < 0) {
            throw new IllegalArgumentException("CSV connector: timestamp column configured as blank, but CSV header does not contain a blank column.");
        }
        if (!timestampBlank && idx < 0) {
            throw new IllegalArgumentException("CSV connector: Timestamp column '" + timestampColumn + "' not found in headers " + headers);
        }
        return idx;
    }

    private static String stripBom(String s) {
        if (s == null) return null;
        if (!s.isEmpty() && s.charAt(0) == '\uFEFF') {
            return s.substring(1);
        }
        return s;
    }

    // CSV parsing with support for quoted fields and commas inside quotes
    private List<String> parseCsvLine(String line) {
        return parseCsvLine(line, columnSeparator);
    }

    private static List<String> parseCsvLine(String line, char separator) {
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
            } else if (ch == separator && !inQuotes) {
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

    private Optional<?> convertCell(String cell, PulseDataType type) {
        try {
            return switch (type) {
                case DOUBLE -> SafeConvert.toDouble(normalizeDecimal(cell)).map(v -> v);
                case LONG -> SafeConvert.toLong(cell).map(v -> v);
                case BOOLEAN -> SafeConvert.toBoolean(cell).map(v -> v);
                case STRING -> Optional.of(cell);
            };
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private long computeOutputTimestamp(long rowTs) {
        if (!timeRealign) {
            return rowTs;
        }
        long dtFile = Math.max(0L, rowTs - fileStartTime);
        return wallStartTime + (long) Math.floor(dtFile / Math.max(0.0000001, speedMultiplier));
    }

    private void emitRow(Map<PulseChannelGroup, List<PulseChannel>> channelsMap,
                         String[] row,
                         long outTms,
                         List<PulseDataPoint> collector) {
        channelsMap.forEach((group, channels) -> {
            for (PulseChannel ch : channels) {
                String columnName = Optional.ofNullable(ch.getSourcePath()).filter(s -> !s.isBlank()).orElse(ch.getPath());
                Integer idx = colIndex.get(columnName);
                if (idx == null) {
                    if (missingColumnsWarned.add(columnName)) {
                        log.warn("CSV connector: column '{}' not found for channel '{}' (group '{}')",
                                columnName, ch.getPath(), group.getCode());
                    }
                    continue;
                }
                String cell = idx < row.length ? row[idx] : null;
                Object valueToEmit;
                if (cell != null && !cell.isEmpty()) {
                    Optional<?> conv = convertCell(cell, ch.getDataType());
                    if (conv.isEmpty()) {
                        lastChannelValues.remove(ch.getPath());
                        continue;
                    }
                    valueToEmit = conv.get();
                    lastChannelValues.put(ch.getPath(), valueToEmit);
                } else {
                    valueToEmit = lastChannelValues.get(ch.getPath());
                    if (valueToEmit == null) {
                        continue;
                    }
                }
                collector.add(PulseDataPoint.builder()
                        .groupCode(group.getCode())
                        .path(ch.getPath())
                        .tms(outTms)
                        .type(ch.getDataType())
                        .val(valueToEmit)
                        .build());
            }
        });
    }
}
