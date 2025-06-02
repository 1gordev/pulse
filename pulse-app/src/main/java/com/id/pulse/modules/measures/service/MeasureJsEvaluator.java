package com.id.pulse.modules.measures.service;

import com.id.pulse.modules.measures.logic.ScriptEvaluatorConsole;
import com.id.pulse.modules.measures.model.ScriptEvaluatorResult;
import com.id.pulse.modules.parser.PulseDataMatrixParser;
import org.graalvm.polyglot.*;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.function.Predicate;

@Service
public class MeasureJsEvaluator {
    private static final Engine ENGINE = Engine.create();

    public static final String JS = "js";


    /**
     * Evaluates the provided JavaScript code with given bindings.
     *
     * @param script       - The JavaScript code to evaluate
     * @param matrixParser - The PulseDataMatrixParser instance to bind to the script
     * @param logContext   - Context for logging, used as a prefix in the console object
     *
     * @return The result of the script evaluation. If the script returns null, this method will return null.
     */
    public ScriptEvaluatorResult evaluate(String script, PulseDataMatrixParser matrixParser, String logContext) {

        // Restrict available classes for Java.type (even stricter)
        Predicate<String> classWhitelist = className ->
                className.startsWith("com.id.pulse.") || className.startsWith("java.util.");

        var logConsole = new ScriptEvaluatorConsole(logContext != null ? logContext : "");
        try (Context context = Context.newBuilder(JS)
                .engine(ENGINE)
                .allowHostAccess(HostAccess.ALL)
                .allowHostClassLookup(classWhitelist)
                .build()) {

            // Make the parser available as "parser"
            Value jsBindings = context.getBindings(JS);
            jsBindings.putMember("_parser", matrixParser);
            jsBindings.putMember("console", logConsole);

            Value result = context.eval(JS, script);

            return ScriptEvaluatorResult.builder()
                    .ok(true)
                    .result(result.isNull() ? null : result.as(Object.class))
                    .logOutput(logConsole.getLogBuffer())
                    .build();
        } catch (PolyglotException e) {
            if(e.getMessage() != null && !e.getMessage().isBlank()) {
                Arrays.stream(e.getMessage().split("\n")).forEach(logConsole::error);
            }
            logConsole.error("Error evaluating JS script", e);
        }

        return ScriptEvaluatorResult.builder()
                .ok(false)
                .logOutput(logConsole.getLogBuffer())
                .build();
    }

    public static class ScriptEvaluationException extends RuntimeException {
        public ScriptEvaluationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
