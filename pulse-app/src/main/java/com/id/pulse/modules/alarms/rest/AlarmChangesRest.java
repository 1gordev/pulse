package com.id.pulse.modules.alarms.rest;

import com.id.pulse.model.PulseRoles;
import com.id.pulse.modules.alarms.PulseAlarm;
import com.id.pulse.modules.alarms.PulseAlarmChange;
import com.id.pulse.modules.alarms.PulseAlarmChangesReq;
import com.id.pulse.modules.alarms.model.PulseAlarmChangeEntity;
import com.id.pulse.modules.alarms.model.PulseAlarmEntity;
import com.id.pulse.modules.alarms.service.AlarmChangesCrudService;
import com.id.px3.crud.IPxAccessControlBase;
import com.id.px3.crud.IPxCrudServiceBase;
import com.id.px3.crud.IPxCrudValidator;
import com.id.px3.crud.PxRestCrudBase;
import com.id.px3.crud.access.PxTokenBasedAccessControl;
import com.id.px3.crud.validation.PxDefaultValidator;
import com.id.px3.rest.security.JwtSecured;
import com.id.px3.rest.security.JwtService;
import jakarta.validation.Validator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("pulse-alarm-changes")
public class AlarmChangesRest extends PxRestCrudBase<PulseAlarmChange, String> {

    private final AlarmChangesCrudService alarmChangesCrudService;
    private final JwtService jwtService;
    private final Validator validator;


    public AlarmChangesRest(AlarmChangesCrudService alarmChangesCrudService, JwtService jwtService, Validator validator) {
        super();
        this.alarmChangesCrudService = alarmChangesCrudService;
        this.jwtService = jwtService;
        this.validator = validator;
    }

    @Override
    protected IPxAccessControlBase<PulseAlarmChange, String> provideAccessControl() {
        return new PxTokenBasedAccessControl<>(
                jwtService,
                List.of(),
                List.of(PulseRoles.PULSE_WRITE),
                List.of(),
                List.of()
        );
    }

    @Override
    protected IPxCrudServiceBase<PulseAlarmChange, PulseAlarmChangeEntity, String> provideCrudService() {
        return alarmChangesCrudService;
    }

    @Override
    protected IPxCrudValidator<PulseAlarmChange> provideValidator() {
        return new PxDefaultValidator<>(validator);
    }

    @PostMapping("find-filtered")
    @JwtSecured
    public ResponseEntity<List<PulseAlarmChange>> findFiltered(@RequestBody PulseAlarmChangesReq req) {
        return ResponseEntity.ok(alarmChangesCrudService.findFiltered(req));
    }

}
