package com.id.pulse.modules.connector.runner.opcua;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.enums.PulseConnectorStatus;
import com.id.pulse.modules.connector.runner.IPulseConnectorRunner;
import com.id.px3.utils.SafeConvert;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class OpcUaConnectorRunner implements IPulseConnectorRunner {

    private final ConcurrentHashMap<String, NodeId> nodeIdMap = new ConcurrentHashMap<>();
    private volatile boolean running = false;
    private OpcUaClient opcUaClient;
    private volatile Thread monitorThread;
    private volatile String endpointUrl;
    private volatile long reconnectSeconds;
    private volatile String batchId;

    @Override
    public PulseConnectorStatus open(PulseConnector connector) {
        this.batchId = UUID.randomUUID().toString();
        this.endpointUrl = Optional.ofNullable(connector.getParams().get("endpointUrl"))
                .map(Object::toString)
                .orElse("");
        this.reconnectSeconds = SafeConvert.toLong(connector.getParams().getOrDefault("reconnectSeconds", 5)).orElse(5L);

        running = true;
        log.debug("Connecting to OPC UA server at {} (retry every {}s)", endpointUrl, reconnectSeconds);
        while (running) {
            try {
                opcUaClient = OpcUaClient.create(endpointUrl);
                opcUaClient = opcUaClient.connect();
                log.info("Connected to OPC UA server at {}", endpointUrl);
                startMonitorIfNeeded();
                return PulseConnectorStatus.CONNECTED;
            } catch (Exception e) {
                // Any failure to connect should be handled gracefully
                log.warn("OPC UA connection attempt failed. Will retry in {}s. Endpoint: {}", reconnectSeconds, endpointUrl, e);
                // Ensure client reference is cleared on failure
                opcUaClient = null;
                try {
                    //noinspection BusyWait
                    Thread.sleep(Math.max(1, reconnectSeconds) * 1000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.info("OPC UA connect retry loop interrupted");
                    break;
                }
            }
        }
        return PulseConnectorStatus.FAILED;
    }

    @Override
    public PulseConnectorStatus close() {
        running = false;
        try {
            log.debug("Closing OPC UA server");
            Thread t = monitorThread;
            if (t != null) {
                try {
                    t.interrupt();
                } catch (Exception ignored) {
                }
            }
            if (opcUaClient != null) {
                opcUaClient.disconnect();
                log.info("Disconnected from OPC UA server");
            } else {
                log.info("OPC UA client was not connected");
            }
            return PulseConnectorStatus.IDLE;
        } catch (UaException e) {
            log.error("Failed to disconnect from OPC UA server", e);
        }
        return PulseConnectorStatus.FAILED;
    }

    private void startMonitorIfNeeded() {
        if (monitorThread != null && monitorThread.isAlive()) return;
        monitorThread = new Thread(() -> {
            while (running) {
                try {
                    if (opcUaClient == null) {
                        try {
                            OpcUaClient client = OpcUaClient.create(endpointUrl);
                            client = client.connect();
                            opcUaClient = client;
                            log.info("Reconnected to OPC UA server at {}", endpointUrl);
                        } catch (Exception e) {
                            log.warn("Reconnection attempt failed. Will retry in {}s. Endpoint: {}", reconnectSeconds, endpointUrl, e);
                            sleepSilently(Math.max(1, reconnectSeconds) * 1000L);
                        }
                        continue;
                    }

                    // Health check: light browse to detect connection loss
                    try {
                        opcUaClient.getAddressSpace().browseNodes(NodeIds.RootFolder);
                    } catch (Exception e) {
                        log.warn("OPC UA connection lost or unhealthy. Will attempt to reconnect.", e);
                        safeDisconnect();
                        opcUaClient = null;
                        sleepSilently(Math.max(1, reconnectSeconds) * 1000L);
                        continue;
                    }

                    // Sleep before next health check
                    sleepSilently(Math.max(1, reconnectSeconds) * 1000L);
                } catch (Throwable t) {
                    log.error("Unexpected error in OPC UA monitor thread", t);
                    sleepSilently(1000L);
                }
            }
        }, "OpcUa-Connector-Monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private void safeDisconnect() {
        try {
            if (opcUaClient != null) {
                opcUaClient.disconnect();
            }
        } catch (Exception e) {
            log.debug("Ignoring exception during disconnect: {}", e.getMessage());
        }
    }

    private void sleepSilently(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean isConnectionClosed(UaException e) {
        try {
            if (e == null || e.getStatusCode() == null) return false;
            long code = e.getStatusCode().getValue();
            return code == StatusCodes.Bad_ConnectionClosed
                   || code == StatusCodes.Bad_SessionClosed
                   || code == StatusCodes.Bad_SecureChannelClosed
                   || code == StatusCodes.Bad_NotConnected
                   || code == StatusCodes.Bad_SessionIdInvalid;
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public CompletableFuture<List<PulseDataPoint>> query(Map<PulseChannelGroup, List<PulseChannel>> channelsMap) {
        return CompletableFuture.supplyAsync(() -> {

            var dataPoints = new ArrayList<PulseDataPoint>();

            if (opcUaClient == null) {
                log.warn("OPC UA client is not connected. Query will return no data.");
                return dataPoints;
            }

            channelsMap.forEach((group, channels) -> {
                // Find NodeId for channels
                channels.forEach(channel -> {
                            var nodeId = nodeIdMap.computeIfAbsent("%s:%s".formatted(group.getCode(), channel.getSourcePath()), k ->
                                    browseAndFindNode(opcUaClient, NodeIds.RootFolder, channel.getSourcePath())
                            );
                            if (nodeId != null) {
                                // Read values
                                try {
                                    var opcValue = opcUaClient.readValue(0, TimestampsToReturn.Server, nodeId);

                                    // Try to convert the read value into the expected type
                                    var optValue = switch (channel.getDataType()) {
                                        case DOUBLE -> SafeConvert.toDouble(opcValue.getValue().getValue());
                                        case LONG -> SafeConvert.toLong(opcValue.getValue().getValue());
                                        case BOOLEAN -> SafeConvert.toBoolean(opcValue.getValue().getValue());
                                        case STRING -> SafeConvert.toString(opcValue.getValue().getValue());
                                    };

                                    // If conversion is successful, add to the results
                                    optValue.ifPresentOrElse(convValue -> {
                                                dataPoints.add(PulseDataPoint.builder()
                                                        .groupCode(group.getCode())
                                                        .path(channel.getPath())
                                                        .val(convValue)
                                                        .tms(getOpcTs(opcValue))
                                                        .type(channel.getDataType())
                                                        .batchId(batchId)
                                                        .build());
                                            },
                                            () -> log.warn("Failed to convert value for channel '%s'".formatted(channel.getPath()))
                                    );

                                } catch (UaException e) {
                                    log.error("Error reading value for channel '%s' in group '%s'".formatted(channel.getPath(), group.getCode()), e);
                                    if (isConnectionClosed(e)) {
                                        log.warn("Detected OPC UA connection closed during read. Marking client as disconnected to trigger reconnect.");
                                        safeDisconnect();
                                        opcUaClient = null;
                                    }
                                }
                            }
                        }
                );
            });

            return dataPoints;
        });
    }

    @Override
    public String getBatchId() {
        return batchId;
    }

    private static long getOpcTs(DataValue opcValue) {
        long opcTs = 0;
        if (opcValue.getSourceTime() != null && opcValue.getSourceTime().isValid()) {
            opcTs = opcValue.getSourceTime().getJavaInstant().toEpochMilli();
        }

        if (opcTs <= 0 && opcValue.getServerTime() != null && opcValue.getServerTime().isValid()) {
            opcTs = opcValue.getServerTime().getJavaInstant().toEpochMilli();
        }

        if (opcTs <= 0) {
            opcTs = System.currentTimeMillis();
        }
        return opcTs;
    }

    private NodeId browseAndFindNode(OpcUaClient client, NodeId browseRoot, String sourcePath) {
        if (sourcePath == null || sourcePath.isEmpty()) {
            return null;
        }
        if (client == null) {
            return null;
        }
        // Remove leading slash if present, then split into segments
        String trimmed = sourcePath.startsWith("/")
                ? sourcePath.substring(1)
                : sourcePath;
        String[] segments = trimmed.split("/");
        return browseAndFindNode(client, browseRoot, segments, 0);
    }

    private NodeId browseAndFindNode(
            OpcUaClient client,
            NodeId currentRoot,
            String[] segments,
            int index
    ) {
        // If we’ve consumed all segments, currentRoot is our target
        if (index >= segments.length) {
            return currentRoot;
        }

        if (client == null) {
            return null;
        }

        String segment = segments[index];
        try {
            // Browse only the direct children of currentRoot
            List<? extends UaNode> children = client
                    .getAddressSpace()
                    .browseNodes(currentRoot);

            for (UaNode child : children) {
                String name = child.getBrowseName().getName();
                if (segment.equals(name)) {
                    // Found this level—recurse into the next
                    NodeId foundId = child.getNodeId();
                    return browseAndFindNode(client, foundId, segments, index + 1);
                }
            }
        } catch (UaException e) {
            log.error("Error browsing nodes under {} at segment '{}'", currentRoot, segment, e);
        }

        // Not found at this level or an error occurred
        return null;
    }

}
