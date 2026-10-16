package com.tapdata.tm.fieldorigin.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
public class FieldOriginQueryResult {
    private String taskId;
    private Long version;
    private String configHash;
    /** requested path -> origins */
    private Map<String, List<FieldOriginDto>> origins = new LinkedHashMap<>();
    private List<String> notFound = new ArrayList<>();
}
