package com.tapdata.tm.metadataInstancesCompare.param;

import lombok.Data;

import java.util.List;

@Data
public class MetadataInstancesApplyParam {
    private String qualifiedName;
    private List<String> fieldNames;
}
