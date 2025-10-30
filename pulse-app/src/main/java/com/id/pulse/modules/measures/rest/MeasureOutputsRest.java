package com.id.pulse.modules.measures.rest;

import com.id.pulse.modules.measures.model.MeasureValueWriteRequest;
import com.id.pulse.modules.measures.service.MeasureOutputWriter;
import com.id.px3.rest.PxRestControllerBase;
import com.id.px3.rest.security.JwtSecured;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("pulse-measures")
@RequiredArgsConstructor
public class MeasureOutputsRest extends PxRestControllerBase {

    private final MeasureOutputWriter measureOutputWriter;

    @PostMapping("write-values")
    @JwtSecured
    public ResponseEntity<Void> writeValues(@RequestBody List<MeasureValueWriteRequest> requests) {
        measureOutputWriter.writeValues(requests);
        return ResponseEntity.accepted().build();
    }
}
