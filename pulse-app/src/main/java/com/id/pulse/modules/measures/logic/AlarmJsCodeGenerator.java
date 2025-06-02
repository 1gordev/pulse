package com.id.pulse.modules.measures.logic;

import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.service.ChannelsCrudService;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseUpStream;
import com.id.pulse.modules.measures.model.enums.PulseSourceType;
import com.id.pulse.modules.measures.service.MeasuresCrudService;
import com.id.pulse.modules.channel.model.enums.PulseDataType;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.*;
import java.util.stream.Collectors;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class AlarmJsCodeGenerator {

    private final ChannelsCrudService channelsCrudService;
    private final MeasuresCrudService measuresCrudService;

    public AlarmJsCodeGenerator(ChannelsCrudService channelsCrudService, MeasuresCrudService measuresCrudService) {
        this.channelsCrudService = channelsCrudService;
        this.measuresCrudService = measuresCrudService;
    }

    public String interpolate(String targetMeasurePath, String engageCondition, String disengageCondition, List<PulseUpStream> upstreams) {
        // Null or blank disengageCondition: invert engage
        boolean hasDisengage = disengageCondition != null && !disengageCondition.isBlank();
        String usedDisengage = hasDisengage ? disengageCondition : "!(" + engageCondition + ")";

        // --- Find types for referenced paths (both engage & disengage) ---
        final Pattern pathPattern = Pattern.compile("\\{\\{\\s*([^}]+?)\\s*}}");
        LinkedHashSet<String> referencedPaths = new LinkedHashSet<>();

        // Extract referenced paths from both conditions
        for (String cond : List.of(engageCondition, usedDisengage)) {
            Matcher matcher = pathPattern.matcher(cond);
            while (matcher.find()) {
                referencedPaths.add(matcher.group(1).trim());
            }
        }

        // Also add the target measure path (for _current)
        referencedPaths.add(targetMeasurePath);

        // Get all unique upstreams plus target (target always treated as a measure)
        List<String> upstreamPaths = upstreams.stream().map(PulseUpStream::getPath).toList();
        var channelsMap = channelsCrudService.findByPaths(
                upstreams.stream()
                        .filter(up -> up.getSourceType() == PulseSourceType.CHANNEL)
                        .map(PulseUpStream::getPath)
                        .toList()
        ).stream().collect(Collectors.toMap(PulseChannel::getPath, ch -> ch));

        var measuresMap = measuresCrudService.findByPaths(
                upstreams.stream()
                        .filter(up -> up.getSourceType() == PulseSourceType.MEASURE)
                        .map(PulseUpStream::getPath)
                        .toList()
        ).stream().collect(Collectors.toMap(PulseMeasure::getPath, m -> m));

        // Add the target as a (boolean) measure if not already present
        if (!measuresMap.containsKey(targetMeasurePath)) {
            measuresMap.put(targetMeasurePath, PulseMeasure.builder().path(targetMeasurePath).dataType(PulseDataType.BOOLEAN).build());
        }

        // --- Map each path to its value type (number, boolean, string) ---
        Map<String, PulseDataType> pathTypeMap = new HashMap<>();
        for (PulseUpStream up : upstreams) {
            var ch = channelsMap.get(up.getPath());
            if (ch != null) {
                pathTypeMap.put(up.getPath(), ch.getDataType());
            } else {
                var m = measuresMap.get(up.getPath());
                if (m != null) {
                    pathTypeMap.put(up.getPath(), m.getDataType());
                } else {
                    throw new IllegalArgumentException("Upstream path not found in channels or measures: " + up.getPath());
                }
            }
        }
        // The target measure is always boolean
        pathTypeMap.put(targetMeasurePath, PulseDataType.BOOLEAN);

        // --- Build JS stub ---
        StringBuilder jsStub = new StringBuilder();
        jsStub.append("// [AlarmJsCodeGenerator]\n");
        // Paths array (for merged timestamps, ignore target)
        var refPathsForTimestamps = referencedPaths.stream()
                .filter(p -> !p.equals(targetMeasurePath))
                .toList();
        jsStub.append("const _paths = [")
                .append(refPathsForTimestamps.stream().map(p -> "'" + p + "'").collect(Collectors.joining(",")))
                .append("];\n");
        jsStub.append("const _timestamps = _parser.toMergedTimestamps(_paths);\n");

        // For each path, create the aligned value array (skip target)
        for (String path : refPathsForTimestamps) {
            PulseDataType type = pathTypeMap.getOrDefault(path, PulseDataType.DOUBLE); // default to DOUBLE
            String varName = "_v_" + toJsSafeVar(path);
            String jsArrayCode = switch (type) {
                case BOOLEAN -> "_parser.toBooleans('" + path + "', false, _timestamps)";
                case STRING -> "_parser.toStrings('" + path + "', '', _timestamps)";
                default -> "_parser.toNumbers('" + path + "', 0.0, _timestamps)";
            };
            jsStub.append("const ").append(varName).append(" = ").append(jsArrayCode).append(";\n");
        }

        // Latest value of the target measure
        jsStub.append("const _current = _parser.toLatestBoolean('").append(targetMeasurePath).append("', false);\n");

        // --- Rewrite conditions ---
        String jsEngage = rewriteCondition(engageCondition, pathPattern, referencedPaths, targetMeasurePath);
        String jsDisengage = rewriteCondition(usedDisengage, pathPattern, referencedPaths, targetMeasurePath);

        // --- JS evaluation functions ---
        jsStub.append("function evaluateEngage() {\n")
                .append("  for (let idx = 0; idx < _timestamps.length; idx++) {\n")
                .append("    if (!(").append(jsEngage).append(")) return false;\n")
                .append("  }\n")
                .append("  return true;\n")
                .append("}\n");

        jsStub.append("function evaluateDisengage() {\n")
                .append("  for (let idx = 0; idx < _timestamps.length; idx++) {\n")
                .append("    if (!(").append(jsDisengage).append(")) return false;\n")
                .append("  }\n")
                .append("  return true;\n")
                .append("}\n");

        // --- Final logic: ---
        jsStub.append("if(_current === false && evaluateEngage()) {\n")
                .append("  true;\n")
                .append("} else if(_current === true && evaluateDisengage()) {\n")
                .append("  false;\n")
                .append("} else {\n")
                .append("  _current;\n")
                .append("}\n");

        return jsStub.toString();
    }

    /**
     * Converts a path to a JS-safe variable name
     */
    private String toJsSafeVar(String path) {
        return path.replaceAll("[^A-Za-z0-9_]", "_");
    }

    /**
     * Rewrites the condition string, replacing {{ path }} with the JS-safe variable usage.
     * Target measure is never replaced, so we can keep logic simple.
     */
    private String rewriteCondition(String condition, Pattern pattern, Set<String> referencedPaths, String targetMeasurePath) {
        Matcher matcher = pattern.matcher(condition);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String rawPath = matcher.group(1).trim();
            // Don't rewrite targetMeasurePath (should never be referenced, but just in case)
            if (rawPath.equals(targetMeasurePath)) {
                matcher.appendReplacement(sb, rawPath);
            } else {
                String safeVar = "_v_" + toJsSafeVar(rawPath) + "[idx]";
                matcher.appendReplacement(sb, safeVar);
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
