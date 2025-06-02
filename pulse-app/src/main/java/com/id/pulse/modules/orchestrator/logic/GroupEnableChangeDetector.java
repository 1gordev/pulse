package com.id.pulse.modules.orchestrator.logic;

import com.id.pulse.modules.channel.model.enums.PulseChannelGroupStatusCode;
import com.id.pulse.modules.channel.service.ChannelGroupsCrudService;
import com.id.pulse.modules.orchestrator.service.ChannelGroupsRegistry;
import com.id.pulse.modules.orchestrator.service.ConnectorsRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
@Slf4j
public class GroupEnableChangeDetector {

    private final ChannelGroupsCrudService channelGroupsCrudService;
    private final ChannelGroupsRegistry channelGroupsRegistry;
    private final ConnectorsRegistry connectorsRegistry;

    public GroupEnableChangeDetector(ChannelGroupsCrudService channelGroupsCrudService,
                                     ChannelGroupsRegistry channelGroupsRegistry,
                                     ConnectorsRegistry connectorsRegistry) {
        this.channelGroupsCrudService = channelGroupsCrudService;
        this.channelGroupsRegistry = channelGroupsRegistry;
        this.connectorsRegistry = connectorsRegistry;
    }

    public void run() {
        // Load all groups
        var groups = channelGroupsCrudService.findAll();

        // Enable or disable groups
        groups.forEach(group -> {
            if (!group.getEnabled()) {
                // Disable group
                if (channelGroupsRegistry.getStatus(group.getCode()) == PulseChannelGroupStatusCode.RUNNING) {
                    log.info("Disabling group: {}", group.getCode());
                    connectorsRegistry.detachGroup(group.getConnectorCode(), group.getCode());
                    channelGroupsRegistry.setGroupStatus(group.getCode(), PulseChannelGroupStatusCode.IDLE);
                }
            } else {
                // Enable group
                if (channelGroupsRegistry.getStatus(group.getCode()) == PulseChannelGroupStatusCode.IDLE) {
                    log.info("Enabling group: {}", group.getCode());
                    connectorsRegistry.attachGroup(group.getConnectorCode(), group.getCode());
                    channelGroupsRegistry.setGroupStatus(group.getCode(), PulseChannelGroupStatusCode.RUNNING);
                }
            }
        });
    }

}
