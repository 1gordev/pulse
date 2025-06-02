package com.id.pulse.modules.orchestrator.service;

import com.id.pulse.modules.channel.model.enums.PulseChannelGroupStatusCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class ChannelGroupsRegistry {

    private final ConcurrentHashMap<String, PulseChannelGroupStatusCode> groupStatusMap = new ConcurrentHashMap<>();

    public PulseChannelGroupStatusCode getStatus(String code) {
        return groupStatusMap.computeIfAbsent(code, k -> PulseChannelGroupStatusCode.IDLE);
    }

    public void setGroupStatus(String code, PulseChannelGroupStatusCode pulseChannelGroupStatusCode) {
        groupStatusMap.put(code, pulseChannelGroupStatusCode);
    }

    public List<String> getAllRunning() {
        return groupStatusMap.entrySet().stream()
                .filter(entry -> entry.getValue() == PulseChannelGroupStatusCode.RUNNING)
                .map(ConcurrentHashMap.Entry::getKey)
                .toList();
    }
}
