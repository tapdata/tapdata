package com.tapdata.tm.trace.param;

import io.tapdata.pdk.apis.entity.QueryOperator;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class TraceFilter {

    private String sql;
    private List<QueryOperator> custom;
    private List<Map<String, Object>> conditions;
}
