package com.id.pulse.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class AppConfig {

    @Value("${pulse.ingestor.write-threads:64}")
    private int ingestorWriteThreads;

    @Value("${pulse.ingestor.queue-size:2048}")
    private int ingestorQueueSize;

    @Value("${pulse.ingestor.chunk-size:1024}")
    private int ingestorChunkSize;

    @Value("${pulse.extractor.read-threads:16}")
    private int extractorReadThreads;

    @Value("${px3.auth.base-url:http://localhost:10181}")
    private String px3AuthBaseUrl;
}
