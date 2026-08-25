package com.tapdata.tm.commons.workflow;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class WorkflowScriptRequest implements Serializable {
    private String runId;
    private String stepId;
    private String stepExecutionId;
    private int attempt;
    private String script;
    private Map<String, Object> context;
    private List<String> connectionIds;
    private long timeoutMs;
    private int maxOutputBytes;
}
