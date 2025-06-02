package com.id.pulse.modules.connector.runner;

import com.id.pulse.model.PulseDataPoint;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.enums.PulseConnectorStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface IPulseConnectorRunner {

    PulseConnectorStatus open(PulseConnector connector);

    PulseConnectorStatus close();

    CompletableFuture<List<PulseDataPoint>> query(Map<PulseChannelGroup, List<PulseChannel>> channelsMap);
}
