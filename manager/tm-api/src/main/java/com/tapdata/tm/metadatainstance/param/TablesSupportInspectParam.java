package com.tapdata.tm.metadatainstance.param;

import lombok.Data;

import java.util.List;

@Data
public class TablesSupportInspectParam {
    private String connectionId;
    private List<String> tableNames;
}
