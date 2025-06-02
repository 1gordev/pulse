package com.id.pulse.modules.measures.service;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.modules.parser.PulseDataMatrixParser;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class MeasureJsEvaluatorTest {

    @Test
    void evaluate_single() throws InterruptedException, ExecutionException {

        MeasureJsEvaluator evaluator = new MeasureJsEvaluator();

        List<Callable<Object>> tasks = new ArrayList<>();

        // Use the builder to create a unique PulseDataMatrix per thread
        PulseDataMatrix matrix = PulseDataMatrix.builder()
                .add("group", "sensor", 1000L, 40.0)
                .build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);
        var sensor = parser.filterByPath("sensor");

        String script = """
                    let pts = _parser.toValues("sensor");
                    console.log(pts);
                    console.log(pts.length);
                    console.log(pts[0]);
                    pts.length === 1 ? pts[0] : -1;
                """;

        var value = evaluator.evaluate(1000L, script, parser, 0d, "single_thread_test");

        assertInstanceOf(Double.class, value.getResult(), "Result should be a Double");
        assertEquals(40.0, value.getResult(), "Result should be 40.0");
    }

    @Test
    void evaluate_single_function() throws InterruptedException, ExecutionException {

        MeasureJsEvaluator evaluator = new MeasureJsEvaluator();

        List<Callable<Object>> tasks = new ArrayList<>();

        // Use the builder to create a unique PulseDataMatrix per thread
        PulseDataMatrix matrix = PulseDataMatrix.builder()
                .add("group", "sensor", 1000L, 40.0)
                .build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);
        var sensor = parser.filterByPath("sensor");

        String script = """
                     function execute() {
                         let pts = _parser.toValues("sensor");
                         console.log(pts);
                         console.log(pts.length);
                         console.log(pts[0]);
                         return pts.length === 1 ? pts[0] : -1;
                     }
                     execute();
                """;

        var value = evaluator.evaluate(1000L, script, parser, 0d, "single_thread_test");

        assertInstanceOf(Double.class, value.getResult(), "Result should be a Double");
        assertEquals(40.0, value.getResult(), "Result should be 40.0");
    }

    @Test
    void evaluate_withSyntaxError_shouldLogAndThrow() {
        MeasureJsEvaluator evaluator = new MeasureJsEvaluator();

        PulseDataMatrix matrix = PulseDataMatrix.builder()
                .add("group", "sensor", 1000L, 40.0)
                .build();
        PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);

        // This script has a syntax error (missing closing parenthesis)
        String script = """
                    function execute( {
                        let pts = parsexxx.toValues("sensor");
                        return pts.length === 1 ? pts[0] : -1;
                    }
                    execute();
                """;

        // The evaluator should throw a ScriptEvaluationException (as per your previous implementation)
        var evalResult = evaluator.evaluate(1000L, script, parser, 0d, "syntax_error_test");
        boolean hasError = evalResult.getLogOutput().stream().anyMatch(log -> log.toLowerCase(Locale.ROOT).contains("error"));

        assertTrue(hasError, "Error message should mention syntax or parse error");
    }

    @Test
    void evaluate_isThreadSafe() throws InterruptedException, ExecutionException {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        MeasureJsEvaluator evaluator = new MeasureJsEvaluator();

        List<Callable<Object>> tasks = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            int idx = i;
            tasks.add(() -> {
                // Use the builder to create a unique PulseDataMatrix per thread
                PulseDataMatrix matrix = PulseDataMatrix.builder()
                        .add("group", "sensor", 1000L + idx, 40.0 + idx)
                        .build();
                PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);


                String script = """
                        let pts = _parser.toValues("sensor");
                        console.log(pts);
                        console.log(pts.length);
                        console.log(pts[0]);
                        pts.length === 1 ? pts[0] : -1;
                        """;

                return evaluator.evaluate(1000L, script, parser, 0d, "multi_thread_test_" + idx).getResult();
            });
        }

        List<Future<Object>> futures = executor.invokeAll(tasks);

        for (int i = 0; i < futures.size(); i++) {
            Object value = futures.get(i).get();
            assertInstanceOf(Double.class, value, "Result should be a Double");
            assertEquals(40.0 + i, value);
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
    }

    @Test
    void evaluate_isThreadSafe_withVirtualThreads() throws InterruptedException, ExecutionException {
        int threadCount = 1000;

        // Java 21+ virtual threads
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            MeasureJsEvaluator evaluator = new MeasureJsEvaluator();
            List<Callable<Object>> tasks = new ArrayList<>();

            for (int i = 0; i < threadCount; i++) {
                int idx = i;
                tasks.add(() -> {
                    PulseDataMatrix matrix = PulseDataMatrix.builder()
                            .add("group", "sensor", 1000L + idx, 40.0 + idx)
                            .build();
                    PulseDataMatrixParser parser = PulseDataMatrixParser.from(matrix);

                    String script = """
                            let pts = _parser.toValues("sensor");
                            // Commented console logs for speed
                            pts.length === 1 ? pts[0] : -1;
                            """;
                    return evaluator.evaluate(1000L, script, parser, 0d, "multi_thread_test_" + idx).getResult();
                });
            }

            // Start all tasks in parallel (virtual threads)
            List<Future<Object>> futures = executor.invokeAll(tasks);

            for (int i = 0; i < futures.size(); i++) {
                Object value = futures.get(i).get();
                assertInstanceOf(Double.class, value, "Result should be a Double");
                assertEquals(40.0 + i, value);
            }
        }
    }
}
