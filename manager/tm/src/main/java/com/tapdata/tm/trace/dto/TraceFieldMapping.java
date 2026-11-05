package com.tapdata.tm.trace.dto;

import lombok.Data;

@Data
public class TraceFieldMapping {

    private String originName;
    private String currentName;
    private String downStreamName;
}
