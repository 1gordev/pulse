package com.id.pulse.modules.measures.logic;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import com.id.pulse.modules.channel.service.ChannelsCrudService;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseUpStream;
import com.id.pulse.modules.measures.model.enums.PulseSourceType;
import com.id.pulse.modules.measures.service.MeasureJsEvaluator;
import com.id.pulse.modules.measures.service.MeasuresCrudService;
import com.id.pulse.modules.parser.PulseDataMatrixParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AlarmJsCodeGeneratorTest {

    private ChannelsCrudService channelsCrudService;
    private MeasuresCrudService measuresCrudService;
    private AlarmJsCodeGenerator generator;

    private static final String TARGET_PATH = "ALARM_FLAG";

    @BeforeEach
    void setup() {
        channelsCrudService = Mockito.mock(ChannelsCrudService.class);
        measuresCrudService = Mockito.mock(MeasuresCrudService.class);
        generator = new AlarmJsCodeGenerator(channelsCrudService, measuresCrudService);
    }

    @Test
    void generatesNumberAndBooleanJSForChannels() {
        var upTemp = PulseUpStream.builder()
                .path("TEMP_1").sourceType(PulseSourceType.CHANNEL).build();
        var upValve = PulseUpStream.builder()
                .path("VALVE").sourceType(PulseSourceType.CHANNEL).build();

        Mockito.when(channelsCrudService.findByPaths(List.of("TEMP_1", "VALVE")))
                .thenReturn(List.of(
                        PulseChannel.builder().path("TEMP_1").dataType(PulseDataType.DOUBLE).build(),
                        PulseChannel.builder().path("VALVE").dataType(PulseDataType.BOOLEAN).build()
                ));
        Mockito.when(measuresCrudService.findByPaths(List.of())).thenReturn(List.of());

        String engageCondition = "{{TEMP_1}} > 50 && {{VALVE}}";
        String js = generator.generate(engageCondition, 1000000L, null, 1000000L, List.of(upTemp, upValve));
        assertTrue(js.contains("const _v_TEMP_1 = _parser.toNumbers('TEMP_1', 0.0, _timestamps);"));
        assertTrue(js.contains("const _v_VALVE = _parser.toBooleans('VALVE', false, _timestamps);"));
        assertTrue(js.contains("if(_current === false && evaluateEngage())"));
    }

    @Test
    void generatesStringJSForMeasure() {
        var upCode = PulseUpStream.builder()
                .path("CODE").sourceType(PulseSourceType.MEASURE).build();
        Mockito.when(channelsCrudService.findByPaths(List.of())).thenReturn(List.of());
        Mockito.when(measuresCrudService.findByPaths(List.of("CODE")))
                .thenReturn(List.of(PulseMeasure.builder().path("CODE").dataType(PulseDataType.STRING).build()));

        String engageCondition = "{{CODE}} === 'A'";
        String js = generator.generate(engageCondition, 1000000L, null, 1000000L, List.of(upCode));
        assertTrue(js.contains("const _v_CODE = _parser.toStrings('CODE', '', _timestamps);"));
        assertTrue(js.contains("evaluateEngage()"));
    }

    @Test
    void throwsIfPathNotFound() {
        var upX = PulseUpStream.builder()
                .path("NOT_FOUND").sourceType(PulseSourceType.CHANNEL).build();
        Mockito.when(channelsCrudService.findByPaths(List.of("NOT_FOUND"))).thenReturn(List.of());
        Mockito.when(measuresCrudService.findByPaths(List.of())).thenReturn(List.of());

        String engageCondition = "{{NOT_FOUND}} > 0";
        Exception ex = assertThrows(IllegalArgumentException.class, () ->
                generator.generate(engageCondition, 1000000L, null, 1000000L, List.of(upX))
        );
        assertTrue(ex.getMessage().contains("Upstream path not found"));
    }

    @Test
    void supportsSpacesInBraces() {
        var up = PulseUpStream.builder().path("TEMP_1").sourceType(PulseSourceType.CHANNEL).build();
        Mockito.when(channelsCrudService.findByPaths(List.of("TEMP_1")))
                .thenReturn(List.of(PulseChannel.builder().path("TEMP_1").dataType(PulseDataType.DOUBLE).build()));
        Mockito.when(measuresCrudService.findByPaths(List.of())).thenReturn(List.of());

        String engageCondition = "{{    TEMP_1   }} >= 10";
        String js = generator.generate(engageCondition, 1000000L, null, 1000000L, List.of(up));
        assertTrue(js.contains("_v_TEMP_1[idx] >= 10"));
    }

    @Test
    void supportsMultipleUsagesOfSamePath() {
        var up = PulseUpStream.builder().path("TEMP_1").sourceType(PulseSourceType.CHANNEL).build();
        Mockito.when(channelsCrudService.findByPaths(List.of("TEMP_1")))
                .thenReturn(List.of(PulseChannel.builder().path("TEMP_1").dataType(PulseDataType.DOUBLE).build()));
        Mockito.when(measuresCrudService.findByPaths(List.of())).thenReturn(List.of());

        String engageCondition = "{{TEMP_1}} > 0 && {{TEMP_1}} < 100";
        String js = generator.generate(engageCondition, 1000000L, null, 1000000L, List.of(up));
        assertEquals(4, js.split("_v_TEMP_1\\[idx\\]").length - 1);
    }

    @Test
    void supportsChannelPathsWithSlashes() {
        var upCh = PulseUpStream.builder().path("Channels/TEMP_1").sourceType(PulseSourceType.CHANNEL).build();
        Mockito.when(channelsCrudService.findByPaths(List.of("Channels/TEMP_1")))
                .thenReturn(List.of(PulseChannel.builder().path("Channels/TEMP_1").dataType(PulseDataType.DOUBLE).build()));
        Mockito.when(measuresCrudService.findByPaths(List.of())).thenReturn(List.of());

        String engageCondition = "{{Channels/TEMP_1}} > 10";
        String js = generator.generate(engageCondition, 1000000L, null, 1000000L, List.of(upCh));
        assertTrue(js.contains("const _v_Channels_TEMP_1 = _parser.toNumbers('Channels/TEMP_1', 0.0, _timestamps);"));
        assertTrue(js.contains("_v_Channels_TEMP_1[idx] > 10"));
    }

    @Test
    void supportsMeasurePathsWithSlashes() {
        var upMeas = PulseUpStream.builder().path("Measures/Valves/OPEN_PERCENTAGE").sourceType(PulseSourceType.MEASURE).build();
        Mockito.when(channelsCrudService.findByPaths(List.of())).thenReturn(List.of());
        Mockito.when(measuresCrudService.findByPaths(List.of("Measures/Valves/OPEN_PERCENTAGE")))
                .thenReturn(List.of(PulseMeasure.builder().path("Measures/Valves/OPEN_PERCENTAGE").dataType(PulseDataType.DOUBLE).build()));

        String engageCondition = "{{Measures/Valves/OPEN_PERCENTAGE}} >= 80";
        String js = generator.generate(engageCondition, 1000000L, null, 1000000L, List.of(upMeas));
        assertTrue(js.contains("const _v_Measures_Valves_OPEN_PERCENTAGE = _parser.toNumbers('Measures/Valves/OPEN_PERCENTAGE', 0.0, _timestamps);"));
        assertTrue(js.contains("_v_Measures_Valves_OPEN_PERCENTAGE[idx] >= 80"));
    }

    @Test
    void supportsMixOfChannelAndMeasurePathsWithSlashes() {
        var upCh = PulseUpStream.builder().path("Channels/HUMIDITY").sourceType(PulseSourceType.CHANNEL).build();
        var upMeas = PulseUpStream.builder().path("Measures/Valves/OPEN_PERCENTAGE").sourceType(PulseSourceType.MEASURE).build();

        Mockito.when(channelsCrudService.findByPaths(List.of("Channels/HUMIDITY")))
                .thenReturn(List.of(PulseChannel.builder().path("Channels/HUMIDITY").dataType(PulseDataType.DOUBLE).build()));
        Mockito.when(measuresCrudService.findByPaths(List.of("Measures/Valves/OPEN_PERCENTAGE")))
                .thenReturn(List.of(PulseMeasure.builder().path("Measures/Valves/OPEN_PERCENTAGE").dataType(PulseDataType.DOUBLE).build()));

        String engageCondition = "{{Channels/HUMIDITY}} < 40 && {{Measures/Valves/OPEN_PERCENTAGE}} >= 80";
        String js = generator.generate(engageCondition, 1000000L, null, 1000000L, List.of(upCh, upMeas));
        assertTrue(js.contains("const _v_Channels_HUMIDITY = _parser.toNumbers('Channels/HUMIDITY', 0.0, _timestamps);"));
        assertTrue(js.contains("const _v_Measures_Valves_OPEN_PERCENTAGE = _parser.toNumbers('Measures/Valves/OPEN_PERCENTAGE', 0.0, _timestamps);"));
        assertTrue(js.contains("_v_Channels_HUMIDITY[idx] < 40 && _v_Measures_Valves_OPEN_PERCENTAGE[idx] >= 80"));
    }

    // --- Now tests that actually run the script using GraalVM and MeasureJsEvaluator ---

    @Test
    void generatedScriptRunsSuccessfullyOnGraalVM() {
        PulseDataMatrix matrix = PulseDataMatrix.builder()
                .add("main", "TEMP_1", 1000L, 55.0)
                .add("main", "VALVE", 1000L, true)
                .build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);

        var upTemp = PulseUpStream.builder().path("TEMP_1").sourceType(PulseSourceType.CHANNEL).build();
        var upValve = PulseUpStream.builder().path("VALVE").sourceType(PulseSourceType.CHANNEL).build();

        ChannelsCrudService chs = Mockito.mock(ChannelsCrudService.class);
        MeasuresCrudService ms = Mockito.mock(MeasuresCrudService.class);
        Mockito.when(chs.findByPaths(List.of("TEMP_1", "VALVE"))).thenReturn(List.of(
                PulseChannel.builder().path("TEMP_1").dataType(PulseDataType.DOUBLE).build(),
                PulseChannel.builder().path("VALVE").dataType(PulseDataType.BOOLEAN).build()
        ));
        Mockito.when(ms.findByPaths(List.of(TARGET_PATH))).thenReturn(List.of(
                PulseMeasure.builder().path(TARGET_PATH).dataType(PulseDataType.BOOLEAN).build()
        ));
        var generator = new AlarmJsCodeGenerator(chs, ms);

        String engageCondition = "{{TEMP_1}} > 50 && {{VALVE}}";
        String jsScript = generator.generate(engageCondition, 10000L, null, 10000L, List.of(upTemp, upValve));

        MeasureJsEvaluator evaluator = new MeasureJsEvaluator();
        // _current = false
        var result = evaluator.evaluate(1000L, jsScript, parser, false,"test");
        assertTrue(result.isOk(), "Script should evaluate without errors");
        assertTrue(result.getResult() instanceof Boolean && (Boolean) result.getResult(), "Alarm should trigger (true)");
    }

    @Test
    void generatedScriptRunsAndReturnsFalseIfConditionNotMet() {
        PulseDataMatrix matrix = PulseDataMatrix.builder()
                .add("main", "TEMP_1", 1000L, 10.0)
                .add("main", "VALVE", 1000L, false)
                .build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);

        var upTemp = PulseUpStream.builder().path("TEMP_1").sourceType(PulseSourceType.CHANNEL).build();
        var upValve = PulseUpStream.builder().path("VALVE").sourceType(PulseSourceType.CHANNEL).build();

        ChannelsCrudService chs = Mockito.mock(ChannelsCrudService.class);
        MeasuresCrudService ms = Mockito.mock(MeasuresCrudService.class);
        Mockito.when(chs.findByPaths(List.of("TEMP_1", "VALVE"))).thenReturn(List.of(
                PulseChannel.builder().path("TEMP_1").dataType(PulseDataType.DOUBLE).build(),
                PulseChannel.builder().path("VALVE").dataType(PulseDataType.BOOLEAN).build()
        ));
        Mockito.when(ms.findByPaths(List.of(TARGET_PATH))).thenReturn(List.of(
                PulseMeasure.builder().path(TARGET_PATH).dataType(PulseDataType.BOOLEAN).build()
        ));
        var generator = new AlarmJsCodeGenerator(chs, ms);

        String engageCondition = "{{TEMP_1}} > 50 && {{VALVE}}";
        String jsScript = generator.generate(engageCondition, 10000L, null, 10000L, List.of(upTemp, upValve));

        MeasureJsEvaluator evaluator = new MeasureJsEvaluator();
        var result = evaluator.evaluate(1000L, jsScript, parser, false, "test");
        assertTrue(result.isOk(), "Script should evaluate without errors");
        assertFalse((Boolean) result.getResult(), "Alarm should NOT trigger (false)");
    }

    @Test
    void generatedScriptRunsWithTwoSeriesHavingSameTimestamps() {
        PulseDataMatrix matrix = PulseDataMatrix.builder()
                .add("main", "TEMP_1", 1000L, 10.0)
                .add("main", "TEMP_1", 2000L, 20.0)
                .add("main", "TEMP_1", 3000L, 30.0)
                .add("main", "TEMP_1", 4000L, 40.0)
                .add("main", "TEMP_1", 5000L, 50.0)
                .add("main", "TEMP_2", 1000L, 100.0)
                .add("main", "TEMP_2", 2000L, 200.0)
                .add("main", "TEMP_2", 3000L, 300.0)
                .add("main", "TEMP_2", 4000L, 400.0)
                .add("main", "TEMP_2", 5000L, 500.0)
                .build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);

        var upA = PulseUpStream.builder().path("TEMP_1").sourceType(PulseSourceType.CHANNEL).build();
        var upB = PulseUpStream.builder().path("TEMP_2").sourceType(PulseSourceType.CHANNEL).build();

        ChannelsCrudService chs = Mockito.mock(ChannelsCrudService.class);
        MeasuresCrudService ms = Mockito.mock(MeasuresCrudService.class);
        Mockito.when(chs.findByPaths(List.of("TEMP_1", "TEMP_2"))).thenReturn(List.of(
                PulseChannel.builder().path("TEMP_1").dataType(PulseDataType.DOUBLE).build(),
                PulseChannel.builder().path("TEMP_2").dataType(PulseDataType.DOUBLE).build()
        ));
        Mockito.when(ms.findByPaths(List.of(TARGET_PATH))).thenReturn(List.of(
                PulseMeasure.builder().path(TARGET_PATH).dataType(PulseDataType.BOOLEAN).build()
        ));
        var generator = new AlarmJsCodeGenerator(chs, ms);

        String engageCondition = "{{TEMP_1}} * 10 == {{TEMP_2}}";
        String jsScript = generator.generate(engageCondition, 5000L, null, 5000L, List.of(upA, upB));

        MeasureJsEvaluator evaluator = new MeasureJsEvaluator();
        var result = evaluator.evaluate(5000L, jsScript, parser, false, "test");
        assertTrue(result.isOk(), "Script should evaluate without errors");
        assertTrue(result.getResult() instanceof Boolean && (Boolean) result.getResult(),
                "Condition should be true for all timestamps in window");
    }

    @Test
    void generatedScriptRunsWithThreeSeriesWithDifferentTimestamps() {
        var builder = PulseDataMatrix.builder()
                .add("main", "A", 1000L, 1.0)
                .add("main", "A", 2000L, 2.0)
                .add("main", "A", 3000L, 3.0)
                .add("main", "A", 4000L, 4.0)
                .add("main", "B", 1200L, 5.0)
                .add("main", "B", 1800L, 6.0)
                .add("main", "B", 2200L, 7.0)
                .add("main", "B", 2600L, 8.0)
                .add("main", "B", 3200L, 9.0)
                .add("main", "B", 3800L, 10.0)
                .add("main", "B", 4400L, 11.0);
        for (long t = 1000L; t <= 4400L; t += 240L) {
            builder.add("main", "C", t, 100.0 + t);
        }
        PulseDataMatrix matrix = builder.build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);

        var upA = PulseUpStream.builder().path("A").sourceType(PulseSourceType.CHANNEL).build();
        var upB = PulseUpStream.builder().path("B").sourceType(PulseSourceType.CHANNEL).build();
        var upC = PulseUpStream.builder().path("C").sourceType(PulseSourceType.CHANNEL).build();

        ChannelsCrudService chs = Mockito.mock(ChannelsCrudService.class);
        MeasuresCrudService ms = Mockito.mock(MeasuresCrudService.class);
        Mockito.when(chs.findByPaths(List.of("A", "B", "C"))).thenReturn(List.of(
                PulseChannel.builder().path("A").dataType(PulseDataType.DOUBLE).build(),
                PulseChannel.builder().path("B").dataType(PulseDataType.DOUBLE).build(),
                PulseChannel.builder().path("C").dataType(PulseDataType.DOUBLE).build()
        ));
        Mockito.when(ms.findByPaths(List.of(TARGET_PATH))).thenReturn(List.of(
                PulseMeasure.builder().path(TARGET_PATH).dataType(PulseDataType.BOOLEAN).build()
        ));
        var generator = new AlarmJsCodeGenerator(chs, ms);

        String engageCondition = "{{A}} >= 1 && {{B}} < 50 && {{C}} >= 1000";
        String jsScript = generator.generate(engageCondition, 4400L, null, 4400L, List.of(upA, upB, upC));

        MeasureJsEvaluator evaluator = new MeasureJsEvaluator();
        var result = evaluator.evaluate(4400L, jsScript, parser, false, "test");
        assertTrue(result.isOk(), "Script should evaluate without errors");
        assertTrue(result.getResult() instanceof Boolean && (Boolean) result.getResult(),
                "Condition should be true for all merged timestamps in window");
    }
}
