package com.tapdata.tm.trace.param;

import io.tapdata.pdk.apis.entity.QueryOperator;
import lombok.Data;

@Data
public class TraceFilter {

    private String sql;
    private QueryOperator custom;
}
