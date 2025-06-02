package com.id.pulse.modules.orchestrator.service;

import com.id.pulse.modules.connector.model.enums.PulseConnectorStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class ConnectorsRegistry {

    private final ConcurrentHashMap<String, List<String>> connectorGroupsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PulseConnectorStatus> connectorStatusMap = new ConcurrentHashMap<>();

    public void detachGroup(String connectorCode, String code) {
        connectorGroupsMap.computeIfAbsent(connectorCode, k -> new ArrayList<>()).remove(code);
    }

    public void attachGroup(String connectorCode, String code) {
        connectorGroupsMap.computeIfAbsent(connectorCode, k -> new ArrayList<>()).add(code);
    }

    public boolean hasGroups(String code) {
        return !connectorGroupsMap.computeIfAbsent(code, k -> new ArrayList<>()).isEmpty();
    }

    public PulseConnectorStatus getStatus(String code) {
        return connectorStatusMap.computeIfAbsent(code, k -> PulseConnectorStatus.IDLE);
    }

    public void setStatus(String code, PulseConnectorStatus status) {
        connectorStatusMap.put(code, status);
    }
}
