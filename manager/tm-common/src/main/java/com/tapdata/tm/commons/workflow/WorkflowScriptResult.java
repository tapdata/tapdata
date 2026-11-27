package com.tapdata.tm.commons.workflow;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class WorkflowScriptResult implements Serializable {
    private boolean success;
    private String errorCode;
    private String errorMessage;
    private Map<String, Object> output;
    private boolean timeout;
    private boolean outputTooLarge;
}
