package com.tapdata.tm.metadatadefinition.param;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class BatchUpdateParam {
    List<String> id;
    private List<Map<String, String>> listtags;
}
