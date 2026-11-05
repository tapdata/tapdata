package com.tapdata.tm.trace.dto.boodline;

import lombok.Data;

import java.util.List;

@Data
public class TableProperties {
    String rootNodeId;
    String preNodeId;
    String nodeType;
    List<FieldNameMapping> joinKeys;
    List<FieldNameMapping> tablePk;
    List<String> updateConditionField;
    String path;
    String tableType;
}