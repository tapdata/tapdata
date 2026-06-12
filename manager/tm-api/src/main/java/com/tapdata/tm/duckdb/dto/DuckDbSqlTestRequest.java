package com.tapdata.tm.duckdb.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DuckDbSqlTestRequest {
    private String sql;
    private String taskId;
    private String nodeId;
    private Integer rows;
}
