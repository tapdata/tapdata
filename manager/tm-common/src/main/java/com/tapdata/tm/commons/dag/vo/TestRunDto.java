package com.tapdata.tm.commons.dag.vo;

import com.tapdata.tm.commons.dag.process.script.JsNodeConfigParam;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

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
    private String testRunInputEventJson;
    private String sql;
    private int logOutputCount = 100;
    private List<JsNodeConfigParam> scriptParams;
}
