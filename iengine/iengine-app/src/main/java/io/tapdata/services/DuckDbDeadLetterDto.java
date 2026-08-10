package io.tapdata.services;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class DuckDbDeadLetterDto implements Serializable {
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
