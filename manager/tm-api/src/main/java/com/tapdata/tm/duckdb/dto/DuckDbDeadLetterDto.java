package com.tapdata.tm.duckdb.dto;

import lombok.Getter;
import lombok.Setter;
import java.time.LocalDateTime;

@Getter
@Setter
public class DuckDbDeadLetterDto {
    private String sourceId;
    private String tableId;
    private String targetTableName;
    private String payload;
    private String errorMessage;
    private String errorClass;
    private LocalDateTime createdAt;
}
