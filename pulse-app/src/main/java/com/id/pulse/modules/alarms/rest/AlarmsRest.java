package com.id.pulse.modules.alarms.rest;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.model.PulseRoles;
import com.id.pulse.modules.alarms.PulseAlarm;
import com.id.pulse.modules.alarms.model.PulseAlarmEntity;
import com.id.pulse.modules.alarms.service.AlarmsCrudService;
import com.id.pulse.modules.poller.service.LatestValuesBucket;
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
@RequestMapping("pulse-alarms")
public class AlarmsRest extends PxRestCrudBase<PulseAlarm, String> {

    private final AlarmsCrudService alarmsCrudService;
    private final JwtService jwtService;
    private final LatestValuesBucket latestValuesBucket;
    private final Validator validator;

    public AlarmsRest(AlarmsCrudService alarmsCrudService,
                      JwtService jwtService,
                      LatestValuesBucket latestValuesBucket,
                      Validator validator) {
        super();
        this.alarmsCrudService = alarmsCrudService;
        this.jwtService = jwtService;
        this.latestValuesBucket = latestValuesBucket;
        this.validator = validator;
    }

    @Override
    protected IPxAccessControlBase<PulseAlarm, String> provideAccessControl() {
        return new PxTokenBasedAccessControl<>(
                jwtService,
                List.of(),
                List.of(PulseRoles.PULSE_WRITE),
                List.of(),
                List.of()
        );
    }

    @Override
    protected IPxCrudServiceBase<PulseAlarm, PulseAlarmEntity, String> provideCrudService() {
        return alarmsCrudService;
    }

    @Override
    protected IPxCrudValidator<PulseAlarm> provideValidator() {
        return new PxDefaultValidator<>(validator);
    }

    @GetMapping("latest")
    @JwtSecured
    public ResponseEntity<PulseDataMatrix> getLatest() {
        return ResponseEntity.ok(latestValuesBucket.readAsMatrix());
    }

    @GetMapping("find-all-referring-physical-asset/{physicalAssetId}")
    @JwtSecured
    public ResponseEntity<List<PulseAlarm>> findAllReferringPhysicalAsset(String physicalAssetId) {
        return ResponseEntity.ok(alarmsCrudService.findAllReferringPhysicalAsset(physicalAssetId));
    }

    @PostMapping("find-by-path")
    @JwtSecured
    public ResponseEntity<PulseAlarm> findByPath(@RequestBody Map<String, String> path) {
        return ResponseEntity.ok(alarmsCrudService.findByPath(path.get("path")).orElse(null));
    }

}
