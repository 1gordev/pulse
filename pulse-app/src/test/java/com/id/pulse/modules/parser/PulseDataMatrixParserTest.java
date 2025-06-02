package com.id.pulse.modules.parser;

import static org.junit.jupiter.api.Assertions.*;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.model.PulseDataPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class PulseDataMatrixParserTest {

    private PulseDataMatrix matrix;
    private PulseDataMatrixParser parser;

    @BeforeEach
    void setup() {
        matrix = PulseDataMatrix.builder()
                // TEMP_1: Dense values
                .add("main", "TEMP_1", 1000L, 10.0)
                .add("main", "TEMP_1", 2000L, 20.0)
                .add("main", "TEMP_1", 3000L, 30.0)
                .add("main", "TEMP_1", 4000L, 40.0)
                .add("main", "TEMP_1", 5000L, 50.0)
                // TEMP_2: Sparse
                .add("main", "TEMP_2", 2500L, 12.5)
                .add("main", "TEMP_2", 4500L, 22.5)
                // TEMP_3: Offset series
                .add("main", "TEMP_3", 1100L, 11.0)
                .add("main", "TEMP_3", 2100L, 21.0)
                .add("main", "TEMP_3", 3100L, 31.0)
                // HUMIDITY: Only a single value
                .add("main", "HUMIDITY", 2000L, 55.0)
                // VALVE: Booleans, out of phase
                .add("main", "VALVE", 1000L, true)
                .add("main", "VALVE", 3500L, false)
                // CODE: Strings, random order
                .add("main", "CODE", 2000L, "A")
                .add("main", "CODE", 4000L, "B")
                .add("main", "CODE", 8000L, "C")
                .build();
        parser = PulseDataMatrixParser.from(matrix);
    }

    @Test
    void testMergedTimestampsAcrossAllSeries() {
        List<Long> merged = parser.toMergedTimestamps(List.of("TEMP_1", "TEMP_2", "TEMP_3", "HUMIDITY", "VALVE", "CODE"));
        assertIterableEquals(
                List.of(1000L, 1100L, 2000L, 2100L, 2500L, 3000L, 3100L, 3500L, 4000L, 4500L, 5000L, 8000L),
                merged
        );
    }

    @Test
    void testToNumbersWithAlignment() {
        List<Long> merged = parser.toMergedTimestamps(List.of("TEMP_1", "TEMP_2"));
        List<Double> vals1 = parser.toNumbers("TEMP_1", -1.0, merged);
        List<Double> vals2 = parser.toNumbers("TEMP_2", -9.0, merged);

        // TEMP_1 values aligned to merged
        assertEquals(List.of(
                10.0, // 1000L exact
                20.0, // 2000L exact
                20.0, // 2500L nearest 2000 (equidistant: 2000/3000, code picks 2000)
                30.0, // 3000L exact
                40.0, // 4000L exact
                40.0, // 4500L nearest 4000 (equidistant: 4000/5000, code picks 4000)
                50.0  // 5000L exact
        ), vals1);

        // TEMP_2 values aligned to merged
        assertEquals(List.of(
                12.5, // 1000L nearest 2500
                12.5, // 2000L nearest 2500
                12.5, // 2500L exact
                12.5, // 3000L nearest 2500
                22.5, // 4000L nearest 2500
                22.5, // 4500L exact
                22.5  // 5000L nearest 4500
        ), vals2);
    }

    @Test
    void testToBooleansWithAlignment() {
        List<Long> merged = List.of(1000L, 1100L, 2000L, 2100L, 2500L, 3000L, 3100L, 3500L, 4000L, 4500L, 5000L, 8000L);
        List<Boolean> vals = parser.toBooleans("VALVE", false, merged);

        assertEquals(merged.size(), vals.size());
        assertEquals(true, vals.get(0));   // 1000L, exact
        assertEquals(true, vals.get(1));   // 1100L, nearest 1000
        assertEquals(true, vals.get(2));   // 2000L, nearest 1000
        assertEquals(true, vals.get(3));   // 2100L, nearest 3500
        assertEquals(false, vals.get(4));  // 2500L, nearest 3500
        assertEquals(false, vals.get(5));  // 3000L, nearest 3500
        assertEquals(false, vals.get(6));  // 3100L, nearest 3500
        assertEquals(false, vals.get(7));  // 3500L, exact
        assertEquals(false, vals.get(8));  // 4000L, nearest 3500
        assertEquals(false, vals.get(9));  // 4500L, nearest 3500
        assertEquals(false, vals.get(10)); // 5000L, nearest 3500
        assertEquals(false, vals.get(11)); // 8000L, nearest 3500
    }

    @Test
    void testToStringsWithAlignment() {
        List<Long> merged = List.of(1000L, 1100L, 2000L, 2100L, 2500L, 3000L, 3100L, 3500L, 4000L, 4500L, 5000L, 8000L);
        List<String> vals = parser.toStrings("CODE", "DEF", merged);

        assertEquals(merged.size(), vals.size());
        assertEquals("A", vals.get(0)); // 1000L -> nearest 2000
        assertEquals("A", vals.get(1)); // 1100L -> nearest 2000
        assertEquals("A", vals.get(2)); // 2000L exact
        assertEquals("A", vals.get(3)); // 2100L -> nearest 2000
        assertEquals("A", vals.get(4)); // 2500L -> nearest 2000
        assertEquals("A", vals.get(5)); // 3000L -> nearest 2000 (tie with 4000, code picks 2000)
        assertEquals("B", vals.get(6)); // 3100L -> nearest 4000
        assertEquals("B", vals.get(7)); // 3500L -> nearest 4000
        assertEquals("B", vals.get(8)); // 4000L exact
        assertEquals("B", vals.get(9)); // 4500L -> nearest 4000
        assertEquals("B", vals.get(10));// 5000L -> nearest 4000
        assertEquals("C", vals.get(11));// 8000L exact
    }

    @Test
    void testToNumbersHandlesNullsAndNoData() {
        PulseDataMatrix matrix = PulseDataMatrix.builder()
                .add("main", "TEMP_X", 1000L, null)
                .add("main", "TEMP_X", 2000L, 0.0)
                .build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);

        // With no alignment, check null is defaulted
        assertEquals(List.of(-1.0, 0.0), parser.toNumbers("TEMP_X", -1.0));

        // Alignment to arbitrary timestamps, when path missing
        List<Long> ts = List.of(1500L, 2500L);
        assertEquals(List.of(-1.0, -1.0), parser.toNumbers("NOT_A_PATH", -1.0, ts));
    }

    @Test
    void testFilterByPathReturnsCorrectly() {
        List<PulseDataPoint> points = parser.filterByPath("TEMP_1");
        assertEquals(5, points.size());
        assertEquals("TEMP_1", points.get(0).getPath());
        assertEquals(10.0, points.get(0).getVal());
        assertEquals(50.0, points.get(4).getVal());
    }

    @Test
    void testEmptyMatrix() {
        PulseDataMatrix empty = PulseDataMatrix.builder().build();
        PulseDataMatrixParser emptyParser = PulseDataMatrixParser.from(empty);

        assertTrue(emptyParser.toTimestamps("ANY").isEmpty());
        assertTrue(emptyParser.toNumbers("ANY", 0.0).isEmpty());
        assertTrue(emptyParser.toBooleans("ANY", false).isEmpty());
        assertTrue(emptyParser.toStrings("ANY", "X").isEmpty());
    }

    @Test
    void testAllDefaultsWhenNoData() {
        PulseDataMatrix onlyOther = PulseDataMatrix.builder()
                .add("main", "OTHER", 5000L, 777.0)
                .build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(onlyOther);

        List<Long> merged = List.of(1L, 2L, 3L, 10L, 20L);
        assertEquals(List.of(0.0, 0.0, 0.0, 0.0, 0.0), parser.toNumbers("TEMP_1", 0.0, merged));
        assertEquals(List.of(false, false, false, false, false), parser.toBooleans("VALVE", false, merged));
        assertEquals(List.of("DEF", "DEF", "DEF", "DEF", "DEF"), parser.toStrings("CODE", "DEF", merged));
    }
}
