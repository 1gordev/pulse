package com.id.pulse.modules.measures.service;

import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseMeasureRegisterHook;
import com.id.pulse.modules.measures.model.PulseMeasureRestCompute;
import com.id.pulse.modules.measures.model.PulseMeasureRestComputeResult;
import com.id.pulse.modules.measures.model.enums.PulseMeasureRegisterHookType;
import com.id.px3.model.DefaultRoles;
import com.id.px3.rest.security.JwtService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.*;

@Service
@Slf4j
public class MeasureHookService {

    private final HashMap<String, PulseMeasureRegisterHook> hooksMap = new HashMap<>();
    private final JwtService jwtService;
    private final RestTemplate restTemplate;

    public MeasureHookService(JwtService jwtService, RestTemplate restTemplate) {
        this.jwtService = jwtService;
        this.restTemplate = restTemplate;
    }

    public void registerHook(PulseMeasureRegisterHook hook) {
        hooksMap.put("%s#%s".formatted(hook.getAudienceId(), hook.getType().name()), hook);
    }

    public void runHooks(PulseMeasureRegisterHookType type) {
        runHooks(type, false, null, null);
    }

    public void runHooks(PulseMeasureRegisterHookType type, boolean reprocessing, String reprocessingSessionId) {
        runHooks(type, reprocessing, reprocessingSessionId, null);
    }

    public void runHooks(PulseMeasureRegisterHookType type,
                         boolean reprocessing,
                         String reprocessingSessionId,
                         Long reprocessingTimestamp) {
        hooksMap.values().stream()
                .filter(hook -> hook.getType() == type)
                .forEach(hook -> callHook(hook, reprocessing, reprocessingSessionId, reprocessingTimestamp));
    }

    public List<PulseMeasure> fetchProvideMeasureList() {
        return hooksMap.values().stream()
                .filter(hook -> hook.getType() == PulseMeasureRegisterHookType.PROVIDE_MEASURE_LIST)
                .flatMap(hook -> fetchProvideMeasureList(hook).stream())
                .toList();
    }

    public Optional<Object> computeMeasure(PulseMeasureRestCompute requestPayload) {
        return hooksMap.values().stream()
                .filter(hook -> hook.getType() == PulseMeasureRegisterHookType.COMPUTE_MEASURE)
                .findFirst()
                .flatMap(hook -> computeMeasure(hook, requestPayload));
    }

    private void callHook(PulseMeasureRegisterHook hook,
                          boolean reprocessing,
                          String reprocessingSessionId,
                          Long reprocessingTimestamp) {
        try {
            // Generate a short-lived token for the hook call
            String token = jwtService.generateToken("system", Set.of(DefaultRoles.ROOT), Duration.ofMinutes(1));
            var headers = new HttpHeaders();
            headers.setBearerAuth(token);

            // Prepare the request entity with headers and execute
            PulseMeasureRegisterHook payload = PulseMeasureRegisterHook.builder()
                    .type(hook.getType())
                    .audienceId(hook.getAudienceId())
                    .postEndPoint(hook.getPostEndPoint())
                    .reprocessing(reprocessing)
                    .reprocessingSessionId(reprocessingSessionId)
                    .reprocessingTimestamp(reprocessingTimestamp)
                    .build();

            var request = new HttpEntity<>(payload, headers);
            restTemplate.postForEntity(hook.getPostEndPoint(), request, Void.class);
        } catch (Exception e) {
            log.error("Error calling hook: {}", hook, e);
        }
    }

    private List<PulseMeasure> fetchProvideMeasureList(PulseMeasureRegisterHook hook) {
        try {
            String token = jwtService.generateToken("system", Set.of(DefaultRoles.ROOT), Duration.ofMinutes(1));
            var headers = new HttpHeaders();
            headers.setBearerAuth(token);

            PulseMeasureRegisterHook payload = PulseMeasureRegisterHook.builder()
                    .type(hook.getType())
                    .audienceId(hook.getAudienceId())
                    .postEndPoint(hook.getPostEndPoint())
                    .build();

            var request = new HttpEntity<>(payload, headers);
            var response = restTemplate.exchange(
                    hook.getPostEndPoint(),
                    HttpMethod.POST,
                    request,
                    new ParameterizedTypeReference<List<PulseMeasure>>() {}
            );
            return response.getBody() != null ? response.getBody() : Collections.emptyList();
        } catch (Exception e) {
            log.error("Error calling PROVIDE_MEASURE_LIST hook: {}", hook, e);
            return Collections.emptyList();
        }
    }

    private Optional<Object> computeMeasure(PulseMeasureRegisterHook hook, PulseMeasureRestCompute payload) {
        try {
            String token = jwtService.generateToken("system", Set.of(DefaultRoles.ROOT), Duration.ofMinutes(1));
            var headers = new HttpHeaders();
            headers.setBearerAuth(token);

            var request = new HttpEntity<>(payload, headers);
            var response = restTemplate.exchange(
                    hook.getPostEndPoint(),
                    HttpMethod.POST,
                    request,
                    new ParameterizedTypeReference<PulseMeasureRestComputeResult>() {}
            );
            PulseMeasureRestComputeResult body = response.getBody();
            return Optional.ofNullable(body).map(PulseMeasureRestComputeResult::getValue);
        } catch (Exception e) {
            log.error("Error calling COMPUTE_MEASURE hook: {}", hook, e);
            return Optional.empty();
        }
    }
}
