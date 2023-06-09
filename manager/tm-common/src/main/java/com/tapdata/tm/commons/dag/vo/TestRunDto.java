package com.tapdata.tm.commons.dag.vo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TestRunDto {
    private String taskId;
    private String jsNodeId;
    private String script;
    private String tableName;
    private Integer rows;
    private Long version;
    private int logOutputCount = 100;
}
