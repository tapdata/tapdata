package com.tapdata.tm.trace.dto;

import io.tapdata.pdk.apis.entity.QueryOperator;
import lombok.Data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
public class TraceQueryCondition {

    private String connectionId;
    private String table;
    private boolean sqlMode;
    private String sql;
    private Integer limit = 10;
    private Integer batchSize = 10;
    private Map<String, Object> filters = new LinkedHashMap<>();
    private List<QueryOperator> queryOperators = new ArrayList<>();
    private Map<String, Object> executeParams = new LinkedHashMap<>();
}
