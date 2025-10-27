package com.id.pulse.modules.connector.service;

import com.id.pulse.config.AppConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class ConnectorsUploadManager {

    private final AppConfig appConfig;

    public String getServerPath(String uploadFile) {
        Path baseDir = Path.of(appConfig.getPulseConnectorsUploadFolder());
        Path fullPath = baseDir.resolve(UUID.randomUUID().toString() + uploadFile).normalize();

        try {
            Files.createDirectories(fullPath.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create directories for path: " + fullPath, e);
        }

        return fullPath.toString();
    }

}
