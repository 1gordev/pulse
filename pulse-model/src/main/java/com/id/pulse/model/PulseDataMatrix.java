package com.id.pulse.model;

import com.id.pulse.utils.PulseDataMatrixBuilder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * PulseDataMatrix is a data structure that holds a matrix of data points.
 * It is organized by group code, path, and timestamp (tms).
 * Each entry in the matrix contains the value associated with the corresponding group code, path, and timestamp.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PulseDataMatrix {

    // groupCode, path, tms, value
    private Map<String, Map<String, Map<Long, Object>>> data = new LinkedHashMap<>();

    public static PulseDataMatrixBuilder builder() {
        return new PulseDataMatrixBuilder();
    }

}
