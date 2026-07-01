package com.tapdata.tm.duckdb.dto;

import lombok.Getter;
import lombok.Setter;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class DuckDbSqlTestResponse {
    private boolean success;
    private List<Map<String, Object>> rows;
    private int rowCount;
    private long executionTimeMs;
    private String errorMessage;
    private String errorType;
    private Map<String, Object> data;
}
