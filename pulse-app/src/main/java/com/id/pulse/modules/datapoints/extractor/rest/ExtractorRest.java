package com.id.pulse.modules.datapoints.extractor.rest;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.model.PulseExtractorReq;
import com.id.pulse.modules.datapoints.ingestor.service.DataExtractor;
import com.id.px3.rest.PxRestControllerBase;
import com.id.px3.rest.security.JwtSecured;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("pulse-extractor")
public class ExtractorRest extends PxRestControllerBase {

    private final DataExtractor dataExtractor;

    public ExtractorRest(DataExtractor dataExtractor) {
        this.dataExtractor = dataExtractor;
    }

    @PostMapping("extract")
    @JwtSecured
    public ResponseEntity<PulseDataMatrix> extract(@RequestBody PulseExtractorReq req) {
        return ResponseEntity.ok(dataExtractor.processRequest(req));
    }
}
