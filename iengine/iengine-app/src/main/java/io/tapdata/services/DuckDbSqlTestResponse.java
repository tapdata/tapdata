package io.tapdata.services;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class DuckDbSqlTestResponse implements Serializable {
    private boolean success;
    private String errorMessage;
    private String errorType;
    private List<Map<String, Object>> rows;
    private int rowCount;
    private long executionTimeMs;
    private Map<String, Object> data;
}
