package com.id.pulse.modules.measures.service;

import com.id.pulse.modules.measures.model.PulseMeasureRegisterHook;
import com.id.pulse.modules.measures.model.enums.PulseMeasureRegisterHookType;
import com.id.px3.model.DefaultRoles;
import com.id.px3.rest.security.JwtService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Set;

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
        runHooks(type, false, null);
    }

    public void runHooks(PulseMeasureRegisterHookType type, boolean reprocessing, String reprocessingSessionId) {
        hooksMap.values().stream()
                .filter(hook -> hook.getType() == type)
                .forEach(hook -> callHook(hook, reprocessing, reprocessingSessionId));
    }

    private void callHook(PulseMeasureRegisterHook hook, boolean reprocessing, String reprocessingSessionId) {
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
                    .build();

            var request = new HttpEntity<>(payload, headers);
            restTemplate.postForEntity(hook.getPostEndPoint(), request, Void.class);
        } catch (Exception e) {
            log.error("Error calling hook: {}", hook, e);
        }
    }
}
