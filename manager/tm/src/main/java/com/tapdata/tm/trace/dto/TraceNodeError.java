package com.tapdata.tm.trace.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class TraceNodeError {
    private String code;
    private String message;
    private String field;
    private String upstreamField;
    private String downstreamField;
    private Object expectedValue;
    private Object actualValue;
    private List<String> details = new ArrayList<>();
}
