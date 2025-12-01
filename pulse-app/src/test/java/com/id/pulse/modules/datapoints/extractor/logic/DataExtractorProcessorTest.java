package com.id.pulse.modules.datapoints.extractor.logic;

import com.id.pulse.modules.timeseries.model.PulseChunkMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataExtractorProcessorTest {

    @Test
    void buildMetadataMapPrefersLowestSamplingRate() {
        PulseChunkMetadata slow = new PulseChunkMetadata();
        slow.setPath("Measures/BayesTop12");
        slow.setSamplingRate(60_000L);

        PulseChunkMetadata fast = new PulseChunkMetadata();
        fast.setPath("Measures/BayesTop12");
        fast.setSamplingRate(1_000L);

        Map<String, PulseChunkMetadata> result = DataExtractorProcessor.buildMetadataMap(List.of(slow, fast));

        assertEquals(1, result.size());
        assertTrue(result.containsKey("Measures/BayesTop12"));
        assertEquals(1_000L, result.get("Measures/BayesTop12").getSamplingRate());
    }
}
