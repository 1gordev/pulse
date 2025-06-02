package com.id.pulse.modules.datapoints.ingestor.service;

import com.id.pulse.model.PulseDataMatrix;
import com.id.pulse.model.PulseExtractorReq;
import com.id.pulse.modules.datapoints.extractor.logic.DataExtractorProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataExtractor {

    private final ApplicationContext appCtx;

    public DataExtractor(ApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    public PulseDataMatrix processRequest(PulseExtractorReq req) {
        if (req == null) {
            throw new IllegalArgumentException("Request cannot be null");
        }
        if (req.getPaths() == null || req.getPaths().isEmpty()) {
            throw new IllegalArgumentException("Paths cannot be null or empty");
        }
        if (req.getStart() == null || req.getEnd() == null) {
            throw new IllegalArgumentException("Start and/or End params cannot be null");
        }
        if (req.getStart().isAfter(req.getEnd()) || req.getStart().equals(req.getEnd())) {
            throw new IllegalArgumentException("Start must be before End");
        }

        var matrix = appCtx.getBean(DataExtractorProcessor.class).extract(
                req.getPaths(),
                req.getStart(),
                req.getEnd()
        );

        return matrix;
    }
}
