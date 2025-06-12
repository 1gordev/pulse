package com.id.pulse.modules.channel.rest;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.model.PulseRoles;
import com.id.pulse.modules.channel.model.PulseChannel;
import com.id.pulse.modules.channel.model.PulseChannelEntity;
import com.id.pulse.modules.channel.service.ChannelsCrudService;
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
@RequestMapping("pulse-channels")
public class ChannelsRest extends PxRestCrudBase<PulseChannel, String> {

    private final ChannelsCrudService channelsCrudService;
    private final JwtService jwtService;
    private final LatestValuesBucket latestValuesBucket;
    private final Validator validator;

    public ChannelsRest(ChannelsCrudService channelsCrudService,
                        JwtService jwtService,
                        LatestValuesBucket latestValuesBucket,
                        Validator validator) {
        super();
        this.channelsCrudService = channelsCrudService;
        this.jwtService = jwtService;
        this.latestValuesBucket = latestValuesBucket;
        this.validator = validator;
    }

    @Override
    protected IPxAccessControlBase<PulseChannel, String> provideAccessControl() {
        return new PxTokenBasedAccessControl<>(
                jwtService,
                List.of(),
                List.of(PulseRoles.PULSE_WRITE),
                List.of(),
                List.of()
        );
    }

    @Override
    protected IPxCrudServiceBase<PulseChannel, PulseChannelEntity, String> provideCrudService() {
        return channelsCrudService;
    }

    @Override
    protected IPxCrudValidator<PulseChannel> provideValidator() {
        return new PxDefaultValidator<>(validator);
    }

    @GetMapping("latest")
    @JwtSecured
    public ResponseEntity<PulseDataMatrix> getLatest() {
        return ResponseEntity.ok(latestValuesBucket.readAsMatrix());
    }

    @PostMapping("find-by-path")
    @JwtSecured
    public ResponseEntity<PulseChannel> findByPath(@RequestBody Map<String, String> path) {
        return ResponseEntity.ok(channelsCrudService.findByPath(path.get("path")).orElse(null));
    }
}
