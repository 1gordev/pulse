package com.id.pulse.modules.replay.rest;

import com.id.pulse.modules.replay.model.ReplayJobView;
import com.id.pulse.modules.replay.model.ReplayBatchCleanupResult;
import com.id.pulse.modules.replay.service.ReplayBatchCleanupService;
import com.id.pulse.modules.replay.service.ReplayService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("replay")
public class ReplayRest {

    private final ReplayService replayService;
    private final ReplayBatchCleanupService replayBatchCleanupService;

    public ReplayRest(ReplayService replayService,
                      ReplayBatchCleanupService replayBatchCleanupService) {
        this.replayService = replayService;
        this.replayBatchCleanupService = replayBatchCleanupService;
    }

    @PostMapping("reprocess/{connectorCode}")
    public ReplayJobView startReprocessing(@PathVariable("connectorCode") String connectorCode) {
        return replayService.startReprocessing(connectorCode);
    }

    @GetMapping("reprocess/{jobId}")
    public ReplayJobView getReprocessingJob(@PathVariable("jobId") String jobId) {
        return replayService.findJob(jobId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Replay job not found: " + jobId));
    }

    @GetMapping("reprocess")
    public List<ReplayJobView> listReprocessingJobs() {
        return replayService.listJobs();
    }

    @GetMapping("reprocess/active")
    public Map<String, ReplayJobView> listActiveReprocessingJobs() {
        return replayService.listActiveReplays();
    }

    @GetMapping("reprocess/batch/{batchId}/exists")
    public Map<String, Boolean> batchExists(@PathVariable("batchId") String batchId) {
        return Map.of("exists", replayBatchCleanupService.batchExists(batchId));
    }

    @DeleteMapping("reprocess/batch/{batchId}")
    public ReplayBatchCleanupResult deleteBatch(@PathVariable("batchId") String batchId) {
        return replayBatchCleanupService.deleteBatch(batchId);
    }

    @PostMapping("reprocess/{connectorCode}/cancel")
    public ReplayJobView cancelReprocessing(@PathVariable("connectorCode") String connectorCode) {
        try {
            return replayService.cancelReprocessing(connectorCode);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage(), e);
        }
    }
}
