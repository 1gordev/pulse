package com.id.pulse.modules.connector.rest;

import com.id.pulse.model.PulseRoles;
import com.id.pulse.modules.connector.model.PulseConnector;
import com.id.pulse.modules.connector.model.PulseConnectorEntity;
import com.id.pulse.modules.connector.service.ConnectorsCrudService;
import com.id.pulse.modules.connector.service.ConnectorsUploadManager;
import com.id.px3.crud.*;
import com.id.px3.crud.access.PxTokenBasedAccessControl;
import com.id.px3.crud.validation.PxDefaultValidator;
import com.id.px3.rest.security.JwtService;
import jakarta.validation.Validator;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("pulse-connectors")
public class ConnectorsRest extends PxRestCrudBase<PulseConnector, String> {

    private final ConnectorsCrudService connectorsCrudService;
    private final JwtService jwtService;
    private final Validator validator;
    private final ConnectorsUploadManager connectorsUploadManager;

    public ConnectorsRest(ConnectorsCrudService connectorsCrudService, JwtService jwtService, Validator validator, ConnectorsUploadManager connectorsUploadManager) {
        super();
        this.connectorsCrudService = connectorsCrudService;
        this.jwtService = jwtService;
        this.validator = validator;
        this.connectorsUploadManager = connectorsUploadManager;
    }

    @Override
    protected IPxAccessControlBase<PulseConnector, String> provideAccessControl() {
        return new PxTokenBasedAccessControl<>(
                jwtService,
                List.of(),
                List.of(PulseRoles.PULSE_WRITE),
                List.of(),
                List.of()
        );
    }

    @Override
    protected IPxCrudServiceBase<PulseConnector, PulseConnectorEntity, String> provideCrudService() {
        return connectorsCrudService;
    }

    @Override
    protected IPxCrudValidator<PulseConnector> provideValidator() {
        return new PxDefaultValidator<>(validator);
    }

    @PostMapping(value = "upload-csv", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, String> uploadCsv(@RequestPart("file") MultipartFile file) throws IOException {
        String original = Optional.ofNullable(file.getOriginalFilename()).orElse("upload.csv");
        // Compute a server path using the configured upload folder
        String serverPathStr = connectorsUploadManager.getServerPath(original);
        Path dest = Paths.get(serverPathStr);
        // Ensure parent directory exists
        Path parent = dest.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (InputStream in = file.getInputStream()) {
            Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
        }
        return Map.of("filePath", dest.toAbsolutePath().toString());
    }

    @GetMapping(value = "csv-headers")
    public List<String> getCsvHeaders(@org.springframework.web.bind.annotation.RequestParam("filePath") String filePath) throws IOException {
        if (filePath == null || filePath.isBlank()) {
            return List.of();
        }
        Path path = Paths.get(filePath);
        if (!Files.exists(path) || Files.isDirectory(path)) {
            return List.of();
        }
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String headerLine = reader.readLine();
            if (headerLine == null) return List.of();
            headerLine = stripBom(headerLine);
            return parseCsvLine(headerLine);
        }
    }

    private static String stripBom(String s) {
        if (s == null) return null;
        if (s.startsWith("\uFEFF")) {
            return s.substring(1);
        }
        return s;
    }

    private static List<String> parseCsvLine(String line) {
        if (line == null) return List.of();
        List<String> result = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // Escaped quote
                    current.append('"');
                    i++; // skip next
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());
        // Trim optional surrounding quotes and whitespace
        for (int i = 0; i < result.size(); i++) {
            String v = result.get(i);
            v = v.trim();
            if (v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
                v = v.substring(1, v.length() - 1);
            }
            result.set(i, v);
        }
        return result;
    }
}
