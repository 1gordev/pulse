package com.id.pulse.modules.connector.rest;

import com.id.pulse.model.PulseRoles;
import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.PulseConnectorEntity;
import com.id.pulse.modules.connector.service.ConnectorsCrudService;
import com.id.px3.crud.*;
import com.id.px3.crud.access.PxTokenBasedAccessControl;
import com.id.px3.crud.validation.PxDefaultValidator;
import com.id.px3.rest.security.JwtService;
import jakarta.validation.Validator;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("pulse-connectors")
public class ConnectorsRest extends PxRestCrudBase<PulseConnector, String> {

    private final ConnectorsCrudService connectorsCrudService;
    private final JwtService jwtService;
    private final Validator validator;

    public ConnectorsRest(ConnectorsCrudService connectorsCrudService, JwtService jwtService, Validator validator) {
        super();
        this.connectorsCrudService = connectorsCrudService;
        this.jwtService = jwtService;
        this.validator = validator;
    }

    @Override
    protected IPxAccessControlBase<PulseConnector, String> provideAccessControl() {
        return new PxTokenBasedAccessControl<>(
                jwtService,
                List.of(),
                List.of(PulseRoles.PULSE_WRITE),
                List.of(),
                List.of()
        );
    }

    @Override
    protected IPxCrudServiceBase<PulseConnector, PulseConnectorEntity, String> provideCrudService() {
        return connectorsCrudService;
    }

    @Override
    protected IPxCrudValidator<PulseConnector> provideValidator() {
        return new PxDefaultValidator<>(validator);
    }
}
