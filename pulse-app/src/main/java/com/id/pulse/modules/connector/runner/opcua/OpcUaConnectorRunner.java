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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class OpcUaConnectorRunner implements IPulseConnectorRunner {

    private final ConcurrentHashMap<String, NodeId> nodeIdMap = new ConcurrentHashMap<>();
    private OpcUaClient opcUaClient;

    @Override
    public PulseConnectorStatus open(PulseConnector connector) {
        try {
            var endpointUrl = Optional.ofNullable(connector.getParams().get("endpointUrl")).map(Object::toString).orElse("");
            log.debug("Connecting to OPC UA server at {}", endpointUrl);
            opcUaClient = OpcUaClient.create(Optional.ofNullable(connector.getParams().get("endpointUrl")).map(Object::toString).orElse(""));
            opcUaClient = opcUaClient.connect();
            log.info("Connected to OPC UA server at {}", endpointUrl);
            return PulseConnectorStatus.CONNECTED;
        } catch (UaException e) {
            log.error("Failed to connect to OPC UA server", e);
        }
        return PulseConnectorStatus.FAILED;
    }

    @Override
    public PulseConnectorStatus close() {
        try {
            log.debug("Closing OPC UA server");
            opcUaClient.disconnect();
            log.info("Disconnected from OPC UA server");
            return PulseConnectorStatus.IDLE;
        } catch (UaException e) {
            log.error("Failed to disconnect from OPC UA server", e);
        }
        return PulseConnectorStatus.FAILED;
    }

    @Override
    public CompletableFuture<List<PulseDataPoint>> query(Map<PulseChannelGroup, List<PulseChannel>> channelsMap) {
        return CompletableFuture.supplyAsync(() -> {

            var dataPoints = new ArrayList<PulseDataPoint>();

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
                                                        .build());
                                            },
                                            () -> log.warn("Failed to convert value for channel '%s'".formatted(channel.getPath()))
                                    );

                                } catch (UaException e) {
                                    log.error("Error reading value for channel '%s' in group '%s'".formatted(channel.getPath(), group.getCode()), e);
                                }
                            }
                        }
                );
            });

            return dataPoints;
        });
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

