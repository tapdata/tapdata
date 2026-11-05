package com.tapdata.tm.trace.param;

import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Data;

import java.util.List;

@Data
public class WideTableTraceRequest {

    private String connectionId;
    private String table;
    private TraceFilter filters;
    @JsonAlias("tracedFields")
    private List<String> trackedFields;
}
