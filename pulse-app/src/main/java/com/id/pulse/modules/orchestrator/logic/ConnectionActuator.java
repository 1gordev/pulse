package com.id.pulse.modules.orchestrator.logic;

import com.id.pulse.modules.channel.model.enums.PulseChannelGroupStatusCode;
import com.id.pulse.modules.connector.model.enums.PulseConnectorStatus;
import com.id.pulse.modules.connector.service.ConnectionManager;
import com.id.pulse.modules.connector.service.ConnectorsCrudService;
import com.id.pulse.modules.orchestrator.service.ConnectorsRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class ConnectionActuator {

    private final ConnectionManager connectionManager;
    private final ConnectorsCrudService connectorsCrudService;
    private final ConnectorsRegistry connectorsRegistry;

    public ConnectionActuator(ConnectionManager connectionManager, ConnectorsCrudService connectorsCrudService, ConnectorsRegistry connectorsRegistry) {
        this.connectionManager = connectionManager;
        this.connectorsCrudService = connectorsCrudService;
        this.connectorsRegistry = connectorsRegistry;
    }

    public void run() {
        // Load all connectors
        var connectors = connectorsCrudService.findAll();

        // Connect or disconnect
        connectors.forEach(pulseConn -> {
            if(connectorsRegistry.hasGroups(pulseConn.getCode())) {
                // Connect
                if (connectorsRegistry.getStatus(pulseConn.getCode()) == PulseConnectorStatus.IDLE) {
                    log.info("Connecting: {}", pulseConn.getCode());
                    connectorsRegistry.setStatus(pulseConn.getCode(), PulseConnectorStatus.CONNECTING);
                    connectionManager.initiateConnection(pulseConn.getCode());
                }
            } else {
                // Disconnect
                if(connectorsRegistry.getStatus(pulseConn.getCode()) == PulseConnectorStatus.CONNECTED) {
                    log.info("Disconnecting: {}", pulseConn.getCode());
                    connectorsRegistry.setStatus(pulseConn.getCode(), PulseConnectorStatus.DISCONNECTING);
                    connectionManager.terminateConnection(pulseConn.getCode());
                }
            }
        });
    }

}
