package com.tapdata.tm.trace.dto;

import io.tapdata.pdk.apis.entity.QueryOperator;
import lombok.Data;

import java.util.*;

@Data
public class TraceQueryCondition {

    private String connectionId;
    private String table;
    private boolean sqlMode;
    private String sql;
    private Integer limit = 10;
    private Integer batchSize = 10;
    private List<Map<String, Object>> filters = new ArrayList<>();
    private List<QueryOperator> queryOperators = new ArrayList<>();
    private Map<String, Object> executeParams = new LinkedHashMap<>();
    private Set<String> conditionKeys = new HashSet<>();
}
