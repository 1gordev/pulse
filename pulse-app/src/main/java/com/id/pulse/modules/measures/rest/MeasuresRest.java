package com.id.pulse.modules.measures.rest;

import com.id.pulse.model.*;
import com.id.pulse.modules.measures.model.PulseMeasure;
import com.id.pulse.modules.measures.model.PulseMeasureEntity;
import com.id.pulse.modules.measures.model.PulseMeasureRegisterHook;
import com.id.pulse.modules.measures.service.MeasureHookService;
import com.id.pulse.modules.measures.service.MeasureTransformerManager;
import com.id.pulse.modules.measures.service.MeasuresCrudService;
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
@RequestMapping("pulse-measures")
public class MeasuresRest extends PxRestCrudBase<PulseMeasure, String> {

    private final MeasuresCrudService measuresCrudService;
    private final MeasureHookService measureHookService;
    private final MeasureTransformerManager measureTransformerManager;
    private final JwtService jwtService;
    private final LatestValuesBucket latestValuesBucket;
    private final Validator validator;

    public MeasuresRest(MeasuresCrudService measuresCrudService, MeasureHookService measureHookService,
                        MeasureTransformerManager measureTransformerManager,
                        JwtService jwtService,
                        LatestValuesBucket latestValuesBucket,
                        Validator validator) {
        super();
        this.measuresCrudService = measuresCrudService;
        this.measureHookService = measureHookService;
        this.measureTransformerManager = measureTransformerManager;
        this.jwtService = jwtService;
        this.latestValuesBucket = latestValuesBucket;
        this.validator = validator;
    }

    @Override
    protected IPxAccessControlBase<PulseMeasure, String> provideAccessControl() {
        return new PxTokenBasedAccessControl<>(
                jwtService,
                List.of(),
                List.of(PulseRoles.PULSE_WRITE),
                List.of(),
                List.of()
        );
    }

    @Override
    protected IPxCrudServiceBase<PulseMeasure, PulseMeasureEntity, String> provideCrudService() {
        return measuresCrudService;
    }

    @Override
    protected IPxCrudValidator<PulseMeasure> provideValidator() {
        return new PxDefaultValidator<>(validator);
    }

    @GetMapping("latest")
    @JwtSecured
    public ResponseEntity<PulseDataMatrix> getLatest() {
        return ResponseEntity.ok(latestValuesBucket.readAsMatrix());
    }

    @GetMapping("find-all-referring-physical-asset/{physicalAssetId}")
    @JwtSecured
    public ResponseEntity<List<PulseMeasure>> findAllReferringPhysicalAsset(@PathVariable("physicalAssetId") String physicalAssetId) {
        return ResponseEntity.ok(measuresCrudService.findAllReferringPhysicalAsset(physicalAssetId));
    }

    @GetMapping("find-all-referring-virtual-asset/{virtualAssetId}")
    @JwtSecured
    public ResponseEntity<List<PulseMeasure>> findAllReferringVirtualAsset(@PathVariable("virtualAssetId") String virtualAssetId) {
        return ResponseEntity.ok(measuresCrudService.findAllReferringVirtualAsset(virtualAssetId));
    }

    @PostMapping("test-measure-transform")
    @JwtSecured
    public ResponseEntity<PulseTestMeasureTransformRes> testMeasureTransform(@RequestBody PulseTestMeasureTransformReq req) {
        return ResponseEntity.ok(measureTransformerManager.testTransform(req.getMeasurePath(), req.getScript(), req.getCurrentValue(), req.getTestData()));
    }

    @PostMapping("find-by-path")
    @JwtSecured
    public ResponseEntity<PulseMeasure> findByPath(@RequestBody Map<String, String> path) {
        return ResponseEntity.ok(measuresCrudService.findByPath(path.get("path")).orElse(null));
    }

    @PostMapping("register-hook")
    @JwtSecured
    public ResponseEntity<Void> registerMeasureHook(@RequestBody PulseMeasureRegisterHook req) {
        measureHookService.registerHook(req);
        return ResponseEntity.ok().build();
    }
}
