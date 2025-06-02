package com.id.pulse.modules.datapoints.model;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.enums.PulseAggregationType;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DpAccumulatorTest {

    private static final String GROUP = "grp";
    private static final String PATH = "path";
    private static final long START = 0L;
    private static final long END = 100L;

    private DpAccumulator accumulator;

    @BeforeEach
    void setUp() {
        accumulator = new DpAccumulator(GROUP, PATH, START, END);
    }

    @Test
    void aggregateOnEmptyShouldReturnNull() {
        assertNull(accumulator.aggregate(PulseAggregationType.COPY));
    }

    @Test
    void testCopyAggregation() {
        PulseDataPoint first = newPoint(1.0);
        PulseDataPoint second = newPoint(2.0);
        accumulator.push(first);
        accumulator.push(second);
        PulseDataPoint result = accumulator.aggregate(PulseAggregationType.COPY);
        assertSame(second, result);
    }

    @Test
    void testMinMaxString() {
        DpAccumulator acc = new DpAccumulator(GROUP, PATH, START, END);
        acc.push(newPoint("beta"));
        acc.push(newPoint("alpha"));
        acc.push(newPoint("gamma"));
        PulseDataPoint min = acc.aggregate(PulseAggregationType.MIN);
        PulseDataPoint max = acc.aggregate(PulseAggregationType.MAX);
        assertEquals("alpha", min.getVal());
        assertEquals("gamma", max.getVal());
    }

    @Test
    void testMinMaxBoolean() {
        DpAccumulator acc = new DpAccumulator(GROUP, PATH, START, END);
        acc.push(newPoint(false));
        acc.push(newPoint(true));
        PulseDataPoint min = acc.aggregate(PulseAggregationType.MIN);
        PulseDataPoint max = acc.aggregate(PulseAggregationType.MAX);
        assertEquals(Boolean.FALSE, min.getVal());
        assertEquals(Boolean.TRUE, max.getVal());
    }

    @Test
    void testSumAndAvgDouble() {
        accumulator.push(newPoint(2.5));
        accumulator.push(newPoint(3.5));
        PulseDataPoint sum = accumulator.aggregate(PulseAggregationType.SUM);
        PulseDataPoint avg = accumulator.aggregate(PulseAggregationType.AVG);
        assertEquals(PulseDataType.DOUBLE, sum.getType());
        assertEquals(6.0, sum.getVal());
        assertEquals(PulseDataType.DOUBLE, avg.getType());
        assertEquals(3.0, avg.getVal());
    }

    @Test
    void testSumAndAvgLong() {
        DpAccumulator acc = new DpAccumulator(GROUP, PATH, START, END);
        acc.push(newLongPoint(1L));
        acc.push(newLongPoint(3L));
        PulseDataPoint sum = acc.aggregate(PulseAggregationType.SUM);
        PulseDataPoint avg = acc.aggregate(PulseAggregationType.AVG);
        assertEquals(PulseDataType.LONG, sum.getType());
        assertEquals(4L, sum.getVal());
        assertEquals(PulseDataType.DOUBLE, avg.getType());
        assertEquals(2.0, avg.getVal());
    }

    @Test
    void testUnsupportedSumForString() {
        DpAccumulator acc = new DpAccumulator(GROUP, PATH, START, END);
        acc.push(newPoint("x"));
        assertThrows(IllegalArgumentException.class,
                () -> acc.aggregate(PulseAggregationType.SUM));
    }

    @Test
    void testConcurrentPushAndSum() throws Exception {
        int threads = 20;
        DpAccumulator acc = new DpAccumulator(GROUP, PATH, START, END);
        // each thread pushes one DOUBLE value of 1.0
        try (ExecutorService exec = Executors.newFixedThreadPool(threads)) {
            List<Future<Void>> futures = new java.util.ArrayList<>();
            Callable<Void> task = () -> {
                acc.push(newPoint(1.0));
                return null;
            };
            for (int i = 0; i < threads; i++) {
                futures.add(exec.submit(task));
            }
            for (Future<Void> f : futures) {
                f.get();
            }
        }
        PulseDataPoint sum = acc.aggregate(PulseAggregationType.SUM);
        assertEquals(20.0, sum.getVal());
    }

    // Helper factories
    private PulseDataPoint newPoint(double val) {
        return PulseDataPoint.builder()
                .groupCode(GROUP)
                .path(PATH)
                .tms(END)
                .type(PulseDataType.DOUBLE)
                .val(val)
                .build();
    }

    private PulseDataPoint newLongPoint(long val) {
        return PulseDataPoint.builder()
                .groupCode(GROUP)
                .path(PATH)
                .tms(END)
                .type(PulseDataType.LONG)
                .val(val)
                .build();
    }

    private PulseDataPoint newPoint(String val) {
        return PulseDataPoint.builder()
                .groupCode(GROUP)
                .path(PATH)
                .tms(END)
                .type(PulseDataType.STRING)
                .val(val)
                .build();
    }

    private PulseDataPoint newPoint(boolean val) {
        return PulseDataPoint.builder()
                .groupCode(GROUP)
                .path(PATH)
                .tms(END)
                .type(PulseDataType.BOOLEAN)
                .val(val)
                .build();
    }
}
