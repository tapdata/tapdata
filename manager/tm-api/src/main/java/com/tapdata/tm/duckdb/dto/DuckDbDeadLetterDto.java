package com.tapdata.tm.duckdb.dto;

import lombok.Getter;
import lombok.Setter;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class DuckDbDeadLetterDto {
    private String id;
    private String contextKey;
    private String targetTableName;
    private List<Map<String, Object>> payload;
    private String taskId;
    private String syncBatchId;
    private String dlqTimestamp;
    private String failedSql;
    private String errorMessage;
    private String errorClass;
    private Integer retryCount;
    private String lastRetryAt;
    private Object manualResolution;
    private String createdAt;
}
