package com.id.pulse.modules.replay.rest;

import com.id.pulse.modules.replay.model.ReplayBatchCleanupResult;
import com.id.pulse.modules.replay.repository.ReplayJobRepository;
import com.id.pulse.modules.replay.service.ReplayBatchCleanupService;
import com.id.pulse.modules.replay.service.ReplayJobStore;
import com.id.pulse.modules.replay.service.ReplayService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ReplayRest.class)
@AutoConfigureMockMvc(addFilters = false)
class ReplayRestTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private ReplayService replayService;

    @MockitoBean
    private ReplayBatchCleanupService replayBatchCleanupService;

    @MockitoBean
    private ReplayJobStore replayJobStore;

    @MockitoBean
    private ReplayJobRepository replayJobRepository;

    @MockitoBean
    private MongoTemplate mongoTemplate;

    @Test
    void batchExistsReturnsFlag() throws Exception {
        when(replayBatchCleanupService.batchExists("batch-1")).thenReturn(true);

        mockMvc.perform(get("/replay/reprocess/batch/batch-1/exists")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.exists").value(true));
    }

    @Test
    void deleteBatchReturnsStats() throws Exception {
        when(replayBatchCleanupService.deleteBatch("batch-1"))
                .thenReturn(new ReplayBatchCleanupResult(3, 1, 1));

        mockMvc.perform(delete("/replay/reprocess/batch/batch-1")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.pointsRemoved").value(3))
                .andExpect(jsonPath("$.docsUpdated").value(1))
                .andExpect(jsonPath("$.docsDeleted").value(1));

        verify(replayBatchCleanupService).deleteBatch("batch-1");
    }
}
