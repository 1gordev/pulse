package com.id.pulse.modules.measures.logic;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.datapoints.ingestor.service.DataIngestor;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseUpStream;
import com.id.pulse.modules.measures.model.enums.PulseSourceType;
import com.id.pulse.modules.measures.model.enums.PulseTransformType;
import com.id.pulse.modules.measures.service.MeasuresCrudService;
import com.id.pulse.modules.measures.model.TransformerRun;
import com.id.pulse.modules.measures.service.MeasureTransformer;
import com.id.pulse.modules.poller.service.LatestValuesBucket;
import com.id.pulse.modules.timeseries.model.PulseChunkMetadata;
import com.id.pulse.modules.timeseries.model.PulseIngestorWriteResult;
import com.mongodb.internal.selector.LatencyMinimizingServerSelector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MeasureTransformerTest {

    @Mock
    private MeasuresCrudService measuresCrudService;
    @Mock
    private DataIngestor dataIngestor;
    @Mock
    private LatestValuesBucket latestValuesBucket;
    @InjectMocks
    private MeasureTransformer measureTransformer;

    @Captor
    private ArgumentCaptor<Map<Long, Object>> tsCaptor;

    @Test
    void testExecuteSumLatestLong() {
        // Setup a single measure M = SUM_LATEST of channel c1
        PulseMeasure m = PulseMeasure.builder()
                .path("M")
                .dataType(PulseDataType.LONG)
                .transformType(PulseTransformType.SUM_LATEST)
                .upstreams(List.of(
                        PulseUpStream.builder().path("c1").sourceType(PulseSourceType.CHANNEL).build()
                ))
                .build();
        when(measuresCrudService.findAll()).thenReturn(List.of(m));

        // Channel data point
        PulseDataPoint dp = PulseDataPoint.builder()
                .path("c1").tms(123L).type(PulseDataType.LONG).val(10L).build();
        TransformerRun run = new TransformerRun(List.of(dp), 123L, 999L);

        // Stub ingestor
        PulseChunkMetadata meta = mock(PulseChunkMetadata.class);
        when(dataIngestor.prepareMetadata(MeasureTransformer.MEASURES_GROUP, "M", PulseDataType.LONG, 999L))
                .thenReturn(meta);
        PulseIngestorWriteResult wr = mock(PulseIngestorWriteResult.class);
        when(dataIngestor.writeAsync(eq(meta), anyMap()))
                .thenReturn(CompletableFuture.completedFuture(wr));

        // Execute
        List<PulseDataPoint> results = measureTransformer.execute(run);

        // Validate returned data point
        assertEquals(1, results.size());
        PulseDataPoint out = results.get(0);
        assertEquals("M", out.getPath());
        assertEquals(123L, out.getTms());
        assertEquals(10L, out.getVal());

        // Verify write to ingestor
        verify(dataIngestor).writeAsync(eq(meta), tsCaptor.capture());
        Map<Long, Object> ts = tsCaptor.getValue();
        assertEquals(1, ts.size());
        assertEquals(10L, ts.get(123L));
    }

    @Test
    void testExecuteAvgLatestDouble() {
        // Setup N = AVG_LATEST of channel c2
        PulseMeasure n = PulseMeasure.builder()
                .path("N")
                .dataType(PulseDataType.DOUBLE)
                .transformType(PulseTransformType.AVG_LATEST)
                .upstreams(List.of(
                        PulseUpStream.builder().path("c2").sourceType(PulseSourceType.CHANNEL).build()
                ))
                .build();
        when(measuresCrudService.findAll()).thenReturn(List.of(n));

        PulseDataPoint dp = PulseDataPoint.builder()
                .path("c2").tms(555L).type(PulseDataType.DOUBLE).val(3.5).build();
        TransformerRun run = new TransformerRun(List.of(dp), 555L, 111L);

        PulseChunkMetadata meta = mock(PulseChunkMetadata.class);
        when(dataIngestor.prepareMetadata(MeasureTransformer.MEASURES_GROUP, "N", PulseDataType.DOUBLE, 111L))
                .thenReturn(meta);
        PulseIngestorWriteResult wr = mock(PulseIngestorWriteResult.class);
        when(dataIngestor.writeAsync(eq(meta), anyMap()))
                .thenReturn(CompletableFuture.completedFuture(wr));

        List<PulseDataPoint> results = measureTransformer.execute(run);
        assertEquals(1, results.size());
        PulseDataPoint out = results.get(0);
        assertEquals("N", out.getPath());
        assertEquals(555L, out.getTms());
        assertEquals(3.5, out.getVal());

        verify(dataIngestor).writeAsync(eq(meta), tsCaptor.capture());
        Map<Long, Object> ts = tsCaptor.getValue();
        assertEquals(1, ts.size());
        assertEquals(3.5, ts.get(555L));
    }

    @Test
    void testExecuteAvgLatestTwoUpstreams() {
        // AVG of c1 and c2
        PulseMeasure m = PulseMeasure.builder()
                .path("AVG")
                .dataType(PulseDataType.DOUBLE)
                .transformType(PulseTransformType.AVG_LATEST)
                .upstreams(List.of(
                        PulseUpStream.builder().path("c1").sourceType(PulseSourceType.CHANNEL).build(),
                        PulseUpStream.builder().path("c2").sourceType(PulseSourceType.CHANNEL).build()
                ))
                .build();
        when(measuresCrudService.findAll()).thenReturn(List.of(m));

        PulseDataPoint dp1 = PulseDataPoint.builder().path("c1").tms(100L).type(PulseDataType.DOUBLE).val(2.0).build();
        PulseDataPoint dp2 = PulseDataPoint.builder().path("c2").tms(100L).type(PulseDataType.DOUBLE).val(4.0).build();
        TransformerRun run = new TransformerRun(List.of(dp1, dp2), 100L, 999L);

        PulseChunkMetadata meta = mock(PulseChunkMetadata.class);
        when(dataIngestor.prepareMetadata(MeasureTransformer.MEASURES_GROUP, "AVG", PulseDataType.DOUBLE, 999L))
                .thenReturn(meta);
        PulseIngestorWriteResult wr = mock(PulseIngestorWriteResult.class);
        when(dataIngestor.writeAsync(eq(meta), anyMap()))
                .thenReturn(CompletableFuture.completedFuture(wr));

        List<PulseDataPoint> results = measureTransformer.execute(run);
        assertEquals(1, results.size());
        assertEquals(3.0, results.get(0).getVal());

        verify(dataIngestor).writeAsync(eq(meta), tsCaptor.capture());
        Map<Long, Object> ts = tsCaptor.getValue();
        assertEquals(1, ts.size());
        assertEquals(3.0, ts.get(100L));
    }

    @Test
    void testExecuteComplexFourLevelTree() {
        // Build measures A-F
        PulseMeasure a = PulseMeasure.builder()
                .path("A").dataType(PulseDataType.DOUBLE).transformType(PulseTransformType.COPY_LATEST)
                .upstreams(List.of(PulseUpStream.builder().path("c1").sourceType(PulseSourceType.CHANNEL).build()))
                .build();
        PulseMeasure b = PulseMeasure.builder()
                .path("B").dataType(PulseDataType.DOUBLE).transformType(PulseTransformType.COPY_LATEST)
                .upstreams(List.of(PulseUpStream.builder().path("c2").sourceType(PulseSourceType.CHANNEL).build()))
                .build();
        PulseMeasure c = PulseMeasure.builder()
                .path("C").dataType(PulseDataType.DOUBLE).transformType(PulseTransformType.AVG_LATEST)
                .upstreams(List.of(
                        PulseUpStream.builder().path("A").sourceType(PulseSourceType.MEASURE).build(),
                        PulseUpStream.builder().path("c2").sourceType(PulseSourceType.CHANNEL).build()
                ))
                .build();
        PulseMeasure d = PulseMeasure.builder()
                .path("D").dataType(PulseDataType.DOUBLE).transformType(PulseTransformType.AVG_LATEST)
                .upstreams(List.of(
                        PulseUpStream.builder().path("B").sourceType(PulseSourceType.MEASURE).build(),
                        PulseUpStream.builder().path("c1").sourceType(PulseSourceType.CHANNEL).build()
                ))
                .build();
        PulseMeasure e = PulseMeasure.builder()
                .path("E").dataType(PulseDataType.DOUBLE).transformType(PulseTransformType.SUM_LATEST)
                .upstreams(List.of(
                        PulseUpStream.builder().path("C").sourceType(PulseSourceType.MEASURE).build(),
                        PulseUpStream.builder().path("D").sourceType(PulseSourceType.MEASURE).build()
                ))
                .build();
        PulseMeasure f = PulseMeasure.builder()
                .path("F").dataType(PulseDataType.DOUBLE).transformType(PulseTransformType.AVG_LATEST)
                .upstreams(List.of(PulseUpStream.builder().path("E").sourceType(PulseSourceType.MEASURE).build()))
                .build();
        when(measuresCrudService.findAll()).thenReturn(List.of(a, b, c, d, e, f));

        // Channel inputs
        PulseDataPoint dp1 = PulseDataPoint.builder().path("c1").tms(1000L).type(PulseDataType.DOUBLE).val(2.0).build();
        PulseDataPoint dp2 = PulseDataPoint.builder().path("c2").tms(1000L).type(PulseDataType.DOUBLE).val(4.0).build();
        TransformerRun run = new TransformerRun(List.of(dp1, dp2), 1000L, 50L);

        // Stub ingestor
        PulseChunkMetadata meta = mock(PulseChunkMetadata.class);
        when(dataIngestor.prepareMetadata(any(), any(), any(), anyLong())).thenReturn(meta);
        PulseIngestorWriteResult wr = mock(PulseIngestorWriteResult.class);
        when(dataIngestor.writeAsync(eq(meta), anyMap())).thenReturn(CompletableFuture.completedFuture(wr));

        // Execute
        List<PulseDataPoint> results = measureTransformer.execute(run);
        assertEquals(6, results.size());
        Map<String, Double> byPath = results.stream()
                .collect(Collectors.toMap(PulseDataPoint::getPath,
                        dp -> ((Number) dp.getVal()).doubleValue()));
        assertEquals(2.0, byPath.get("A"));
        assertEquals(4.0, byPath.get("B"));
        assertEquals(3.0, byPath.get("C"));
        assertEquals(3.0, byPath.get("D"));
        assertEquals(6.0, byPath.get("E"));
        assertEquals(6.0, byPath.get("F"));

        // Verify writes for all
        verify(dataIngestor, times(6)).writeAsync(eq(meta), anyMap());
    }
}
