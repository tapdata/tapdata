package com.tapdata.tm.trace.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class TraceValue {

    private long matchedCount;
    private List<Map<String, Object>> currentRecords = new ArrayList<>();
    private List<Map<String, Object>> downStreamRecords = new ArrayList<>();
    private List<TraceFieldMapping> tracedFields = new ArrayList<>();
    private boolean visable = true;
    private List<TraceFieldMapping> resultFieldMapping = new ArrayList<>();
    private List<Map<String, Object>> queryConditions  = new ArrayList<>();
}
