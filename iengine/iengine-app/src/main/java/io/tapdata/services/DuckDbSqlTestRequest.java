package io.tapdata.services;

import lombok.Data;

import java.io.Serializable;

@Data
public class DuckDbSqlTestRequest implements Serializable {
    private String sql;
    private String taskId;
    private String nodeId;
    private Integer rows;
}
