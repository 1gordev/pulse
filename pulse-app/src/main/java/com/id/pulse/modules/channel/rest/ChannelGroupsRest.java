package com.id.pulse.modules.channel.rest;

import com.id.pulse.model.PulseRoles;
import com.id.pulse.modules.channel.model.PulseChannelGroup;
import com.id.pulse.modules.channel.model.PulseChannelGroupEntity;
import com.id.pulse.modules.channel.service.ChannelGroupsCrudService;
import com.id.px3.crud.IPxAccessControlBase;
import com.id.px3.crud.IPxCrudServiceBase;
import com.id.px3.crud.IPxCrudValidator;
import com.id.px3.crud.PxRestCrudBase;
import com.id.px3.crud.access.PxTokenBasedAccessControl;
import com.id.px3.crud.validation.PxDefaultValidator;
import com.id.px3.rest.security.JwtService;
import jakarta.validation.Validator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("pulse-channel-groups")
public class ChannelGroupsRest extends PxRestCrudBase<PulseChannelGroup, String> {

    private final ChannelGroupsCrudService channelGroupsCrudService;
    private final JwtService jwtService;
    private final Validator validator;

    public ChannelGroupsRest(ChannelGroupsCrudService channelGroupsCrudService, JwtService jwtService, Validator validator) {
        super();
        this.channelGroupsCrudService = channelGroupsCrudService;
        this.jwtService = jwtService;
        this.validator = validator;
    }

    @Override
    protected IPxAccessControlBase<PulseChannelGroup, String> provideAccessControl() {
        return new PxTokenBasedAccessControl<>(
                jwtService,
                List.of(),
                List.of(PulseRoles.PULSE_WRITE),
                List.of(),
                List.of()
        );
    }

    @Override
    protected IPxCrudServiceBase<PulseChannelGroup, PulseChannelGroupEntity, String> provideCrudService() {
        return channelGroupsCrudService;
    }

    @Override
    protected IPxCrudValidator<PulseChannelGroup> provideValidator() {
        return new PxDefaultValidator<>(validator);
    }

    @GetMapping("by-connector/{connectorCode}")
    public List<PulseChannelGroup> findByConnector(@PathVariable("connectorCode") String connectorCode) {
        return channelGroupsCrudService.findByConnectorCode(connectorCode);
    }
}
