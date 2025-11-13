package com.id.pulse.modules.connector.service;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.enums.ConnectorCallReason;
import com.id.pulse.modules.connector.model.enums.PulseConnectorStatus;
import com.id.pulse.modules.connector.model.enums.PulseConnectorType;
import com.id.pulse.modules.connector.runner.IPulseConnectorRunner;
import com.id.pulse.modules.connector.runner.csv.CsvConnectorRunner;
import com.id.pulse.modules.connector.runner.opcua.OpcUaConnectorRunner;
import com.id.pulse.modules.orchestrator.service.ConnectorsRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class ConnectionManager {

    private final ApplicationContext appCtx;
    private final ConnectorsCrudService connectorsCrudService;
    private final ConcurrentHashMap<String, IPulseConnectorRunner> instances = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PulseConnectorType, Class<? extends IPulseConnectorRunner>> runnerClasses = new ConcurrentHashMap<>();
    private final ConnectorsRegistry connectorsRegistry;
    private final Map<String, Long> warnNotFound = new HashMap<>();
    private final ConcurrentHashMap<String, Boolean> replayModes = new ConcurrentHashMap<>();

    public ConnectionManager(ApplicationContext appCtx, ConnectorsCrudService connectorsCrudService, ConnectorsRegistry connectorsRegistry) {
        this.appCtx = appCtx;
        this.connectorsCrudService = connectorsCrudService;

        runnerClasses.put(PulseConnectorType.OPCUA, OpcUaConnectorRunner.class);
        runnerClasses.put(PulseConnectorType.CSV, CsvConnectorRunner.class);
        this.connectorsRegistry = connectorsRegistry;
    }

    @Async
    public void initiateConnection(String code) {
        try {
            // Load the connector
            connectorsCrudService.findByCode(code).ifPresent(connector -> {
                // If there's already an instance, clean it up
                if (instances.containsKey(code)) {
                    var instance = instances.get(code);
                    instance.close();
                    instances.remove(code);
                }

                // Instantiate the runner
                var runnerClass = runnerClasses.get(connector.getType());
                if (runnerClass == null) {
                    log.error("No runner found for type {}", connector.getType());
                    connectorsRegistry.setStatus(code, PulseConnectorStatus.FAILED);
                    return;
                }

                var runnerInstance = appCtx.getBean(runnerClass);
                instances.put(code, runnerInstance);
                Boolean replayMode = replayModes.get(code);
                if (replayMode != null) {
                    try {
                        runnerInstance.setReplayMode(replayMode);
                    } catch (Exception ignored) {
                    }
                }

                // Initialize the runner
                log.trace("Initializing runner: {}", runnerInstance.getClass().getName());
                var newStatus = runnerInstance.open(connector);
                connectorsRegistry.setStatus(code, newStatus);
            });
        } catch (Exception e) {
            log.error("Failed to instantiate runner", e);
            connectorsRegistry.setStatus(code, PulseConnectorStatus.FAILED);
            throw new RuntimeException(e);
        }
    }

    @Async
    public void terminateConnection(String code) {
        // If there's already an instance, clean it up
        if (instances.containsKey(code)) {
            var instance = instances.get(code);
            if (instance.close() == PulseConnectorStatus.FAILED) {
                log.error("Failed to close connection {}", code);
            }
            instances.remove(code);
        }
        // Update the status
        connectorsRegistry.setStatus(code, PulseConnectorStatus.IDLE);
        replayModes.remove(code);
    }

    @Async
    public CompletableFuture<List<PulseDataPoint>> queryConnector(String code,
                                                                  Map<PulseChannelGroup, List<PulseChannel>> channels,
                                                                  ConnectorCallReason reason) {
        // If there's already an instance, clean it up
        if (instances.containsKey(code)) {
            if (connectorsRegistry.getStatus(code) == PulseConnectorStatus.CONNECTED) {
                var instance = instances.get(code);
                return instance.query(channels, reason);
            }
        } else {
            long now = System.currentTimeMillis();
            if (!warnNotFound.containsKey(code) || warnNotFound.get(code) < now) {
                log.warn("No instance found for code {}", code);
                warnNotFound.put(code, now + 10000L);
            }
        }
        return CompletableFuture.completedFuture(List.of());
    }

    public void setReplayMode(String code, boolean replayMode) {
        IPulseConnectorRunner runner = instances.get(code);
        if (runner != null) {
            try {
                runner.setReplayMode(replayMode);
            } catch (Exception ignored) {
            }
        }
        replayModes.put(code, replayMode);
    }

    public boolean isReplayComplete(String code) {
        IPulseConnectorRunner runner = instances.get(code);
        if (runner != null) {
            try {
                return runner.isReplayComplete();
            } catch (Exception ignored) {
            }
        }
        return false;
    }

    public int getReplayProgressPercent(String code) {
        IPulseConnectorRunner runner = instances.get(code);
        if (runner != null) {
            try {
                return runner.getReplayProgressPercent();
            } catch (Exception ignored) {
            }
        }
        return -1;
    }

    public Class<? extends IPulseConnectorRunner> getConnectorClass(String connectorCode) {
        return connectorsCrudService.findByCode(connectorCode)
                .map(PulseConnector::getType)
                .map(runnerClasses::get)
                .orElseGet(() -> {
                    log.error("No connector found for code {}", connectorCode);
                    return null;
                });
    }
}
