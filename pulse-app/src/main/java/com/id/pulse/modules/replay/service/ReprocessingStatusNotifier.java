package com.id.pulse.modules.replay.service;

import com.id.pulse.config.AppConfig;
import com.id.pulse.modules.replay.model.ReprocessingSessionNotification;
import com.id.pulse.modules.replay.model.ReprocessingSessionStatus;
import com.id.px3.model.DefaultRoles;
import com.id.px3.rest.security.JwtService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReprocessingStatusNotifier {

    private static final Duration TOKEN_TTL = Duration.ofMinutes(1);
    private static final String SUBJECT = "pulse-reprocessing-notifier";

    private final JwtService jwtService;
    private final RestTemplate restTemplate;
    private final AppConfig appConfig;

    public void notifyStatus(String sessionId, ReprocessingSessionStatus status) {
        if (!StringUtils.hasText(sessionId) || status == null) {
            return;
        }
        String baseUrl = appConfig.getIrisBackendBaseUrl();
        if (!StringUtils.hasText(baseUrl)) {
            log.warn("IRIS backend base URL is not configured; skipping reprocessing notification.");
            return;
        }
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(jwtService.generateToken(SUBJECT, Set.of(DefaultRoles.ROOT), TOKEN_TTL));

            ReprocessingSessionNotification payload = new ReprocessingSessionNotification(sessionId, status);
            HttpEntity<ReprocessingSessionNotification> request = new HttpEntity<>(payload, headers);
            restTemplate.postForEntity(baseUrl + "/bnet/reprocessing/status", request, Void.class);
        } catch (Exception ex) {
            log.warn("Failed to notify IRIS about reprocessing session {} status {}", sessionId, status, ex);
        }
    }
}
