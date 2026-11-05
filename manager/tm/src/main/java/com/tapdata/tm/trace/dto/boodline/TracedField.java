package com.tapdata.tm.trace.dto.boodline;

import lombok.Data;

@Data
public final class TracedField {
    private String rootNodeId;
    private String rootFieldName;

    public TracedField(String rootNodeId, String rootFieldName) {
        this.rootNodeId = rootNodeId;
        this.rootFieldName = rootFieldName;
    }
}