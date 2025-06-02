package com.id.pulse.modules.measures.logic;

import com.id.pulse.annotations.ExposeToGraal;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class ScriptEvaluatorConsole {

    private final String logContext;

    @Getter
    private final List<String> logBuffer = new ArrayList<>();

    public ScriptEvaluatorConsole(String logContext) {
        this.logContext = logContext;
    }

    @ExposeToGraal
    public void log(Object... args) {
        log.info(join(args));
    }

    public void info(Object... args) {
        log.info(join(args));
    }

    public void warn(Object... args) {
        log.warn(join(args));
    }

    public void error(Object... args) {
        log.error(join(args));
    }

    private String join(Object[] args) {
        List<String> argsList = new ArrayList<>();
        argsList.add(String.valueOf(logContext)); // Insert context as the first value

        if (args != null) {
            Arrays.stream(args)
                    .map(String::valueOf)
                    .forEach(argsList::add);
        } else {
            argsList.add("---");
        }

        // Add the log context as the first element, make sure the log buffer doesn't exceed a reasonable size
        logBuffer.addAll(argsList);
        if (logBuffer.size() > 1000) { // Limit the size of the log buffer
            logBuffer.subList(0, logBuffer.size() - 1000).clear();
        }

        return String.join(" ", argsList);
    }

}
