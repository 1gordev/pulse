package com.id.pulse.modules.poller.service;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.enums.PulseAggregationType;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.datapoints.model.DpAccumulator;
import com.id.pulse.modules.datapoints.service.DpAccumulatorsManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ChannelPollerAggregateTest {

    private static final String GROUP = "group1";
    private static final String PATH = "path1";
    private static final long BASE = 100L;

    private DpAccumulatorsManager dpManager;
    private ChannelPoller poller;

    @BeforeEach
    void setUp() {
        dpManager = new DpAccumulatorsManager();
        // Other dependencies not used in aggregate()
        poller = new ChannelPoller(
                dpManager,
                null, null, null,
                null, null, null, null
        );
    }

    @Test
    void testAggregateRemovesCompletedAccumulators() throws Exception {
        // Create a channel with SUM aggregation
        PulseChannel channel = new PulseChannel();
        channel.setChannelGroupCode(GROUP);
        channel.setPath(PATH);
        channel.setAggregationType(PulseAggregationType.SUM);
        channel.setAggregationTimeBase(BASE);

        List<PulseChannel> channels = List.of(channel);

        // Create two data points in successive windows: [0,100) and [100,200)
        PulseDataPoint dp1 = new PulseDataPoint(GROUP, PATH, 10L, PulseDataType.DOUBLE, 1.0);
        PulseDataPoint dp2 = new PulseDataPoint(GROUP, PATH, 150L, PulseDataType.DOUBLE, 2.0);
        List<PulseDataPoint> dataPoints = List.of(dp1, dp2);

        // Invoke private aggregate via reflection
        Method aggregateMethod = ChannelPoller.class.getDeclaredMethod("aggregate", List.class, List.class);
        aggregateMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<PulseDataPoint> results = (List<PulseDataPoint>) aggregateMethod.invoke(poller, channels, dataPoints);

        // Only the first window [0,100) is completed (tmsAccEnd=100 <= lastKey=150)
        assertEquals(1, results.size(), "Expected one aggregated point for the first window");
        assertEquals(1.0, (double) results.getFirst().getVal(), 1e-6, "Expected one aggregated point for the first window");

        // After aggregation, only the second accumulator remains in manager
        // No completed accumulators for tmsCompare=150
        List<DpAccumulator> completed = dpManager.findCompleted(GROUP, PATH, 150L);
        assertTrue(completed.isEmpty(), "Expected no completed accumulators after removal");

        // But for tmsCompare=200, the second window [100,200) is completed
        List<DpAccumulator> completedSecond = dpManager.findCompleted(GROUP, PATH, 200L);
        assertEquals(1, completedSecond.size(), "Expected second accumulator to be present");
        DpAccumulator remaining = completedSecond.getFirst();
        assertEquals(100L, remaining.getTmsAccStart());
        assertEquals(200L, remaining.getTmsAccEnd());
    }
}
