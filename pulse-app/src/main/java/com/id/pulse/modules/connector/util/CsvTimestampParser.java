package com.id.pulse.modules.connector.util;

import com.id.pulse.modules.connector.model.enums.CsvTimestampFormat;
import com.id.px3.utils.SafeConvert;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public final class CsvTimestampParser {

    private static final Locale DEFAULT_LOCALE = Locale.US;
    private static final Map<CsvTimestampFormat, List<DateTimeFormatter>> FORMATTERS = new EnumMap<>(CsvTimestampFormat.class);

    static {
        register(CsvTimestampFormat.YYYY_MM_DD_HH_MM_SS, "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss");
        register(CsvTimestampFormat.YYYY_MM_DD_HH_MM_SS_SSS, "yyyy-MM-dd HH:mm:ss.SSS", "yyyy/MM/dd HH:mm:ss.SSS");
        register(CsvTimestampFormat.YYYY_MM_DD_HH_MM, "yyyy-MM-dd HH:mm", "yyyy/MM/dd HH:mm");
        register(CsvTimestampFormat.YYYY_MM_DD_HH_MM_AMPM, "yyyy-MM-dd hh:mm a", "yyyy/MM/dd hh:mm a");
        register(CsvTimestampFormat.DD_MM_YYYY_HH_MM_SS, "dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss");
        register(CsvTimestampFormat.DD_MM_YYYY_HH_MM, "dd-MM-yyyy HH:mm", "dd/MM/yyyy HH:mm");
        register(CsvTimestampFormat.DD_MM_YYYY_HH_MM_AMPM, "dd-MM-yyyy hh:mm a", "dd/MM/yyyy hh:mm a");
        register(CsvTimestampFormat.MM_DD_YYYY_HH_MM_SS, "MM-dd-yyyy HH:mm:ss", "MM/dd/yyyy HH:mm:ss");
        register(CsvTimestampFormat.MM_DD_YYYY_HH_MM, "MM-dd-yyyy HH:mm", "MM/dd/yyyy HH:mm");
        register(CsvTimestampFormat.MM_DD_YYYY_HH_MM_AMPM, "MM-dd-yyyy hh:mm a", "MM/dd/yyyy hh:mm a");
    }

    private CsvTimestampParser() {
    }

    public static CsvTimestampFormat resolveFormat(Object raw) {
        if (raw == null) {
            return CsvTimestampFormat.ISO_8601;
        }
        String normalized = raw.toString().trim().toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return CsvTimestampFormat.ISO_8601;
        }
        try {
            return CsvTimestampFormat.valueOf(normalized);
        } catch (IllegalArgumentException ex) {
            return CsvTimestampFormat.ISO_8601;
        }
    }

    public static Long parse(String value, CsvTimestampFormat format) {
        return parse(value, format, 0);
    }

    public static Long parse(String value, CsvTimestampFormat format, int offsetMinutes) {
        Long base = parseInternal(value, format);
        if (base == null) {
            return null;
        }
        if (offsetMinutes == 0) {
            return base;
        }
        return base - offsetMinutes * 60_000L;
    }

    private static Long parseInternal(String value, CsvTimestampFormat format) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return switch (format) {
            case EPOCH_MILLIS -> parseEpochMillis(trimmed).orElse(null);
            case EPOCH_SECONDS -> parseEpochSeconds(trimmed).orElse(null);
            case ISO_8601 -> parseIso(trimmed).orElse(null);
            default -> parseWithFormatters(trimmed, FORMATTERS.get(format)).orElse(null);
        };
    }

    public static int resolveOffsetMinutes(Object raw) {
        if (raw == null) {
            return 0;
        }
        String text = raw.toString().trim();
        if (text.isEmpty()) {
            return 0;
        }
        if (text.length() != 6 || (text.charAt(0) != '+' && text.charAt(0) != '-') || text.charAt(3) != ':') {
            return 0;
        }
        try {
            int hours = Integer.parseInt(text.substring(1, 3));
            int minutes = Integer.parseInt(text.substring(4, 6));
            if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59) {
                return 0;
            }
            int total = hours * 60 + minutes;
            return text.charAt(0) == '-' ? -total : total;
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    private static Optional<Long> parseEpochMillis(String value) {
        return SafeConvert.toLong(value);
    }

    private static Optional<Long> parseEpochSeconds(String value) {
        Optional<Long> seconds = SafeConvert.toLong(value);
        if (seconds.isPresent()) {
            return Optional.of(seconds.get() * 1000L);
        }
        Optional<Double> asDouble = SafeConvert.toDouble(value);
        return asDouble.map(v -> (long) Math.floor(v * 1000L));
    }

    private static Optional<Long> parseIso(String value) {
        try {
            return Optional.of(Instant.parse(value).toEpochMilli());
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    private static Optional<Long> parseWithFormatters(String value, List<DateTimeFormatter> formatters) {
        if (formatters == null || formatters.isEmpty()) {
            return Optional.empty();
        }
        for (DateTimeFormatter formatter : formatters) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(value, formatter);
                return Optional.of(ldt.toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (Exception ignored) {
            }
        }
        return Optional.empty();
    }

    private static void register(CsvTimestampFormat format, String... patterns) {
        List<DateTimeFormatter> formatterList = new ArrayList<>(patterns.length);
        for (String pattern : patterns) {
            formatterList.add(formatter(pattern));
        }
        FORMATTERS.put(format, formatterList);
    }

    private static DateTimeFormatter formatter(String pattern) {
        return DateTimeFormatter.ofPattern(pattern, DEFAULT_LOCALE);
    }
}
