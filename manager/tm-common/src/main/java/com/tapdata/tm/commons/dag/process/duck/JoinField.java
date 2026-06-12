package com.tapdata.tm.commons.dag.process.duck;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class JoinField {

    private String table;

    private String field;

    public Map<String, String> toMap() {
        Map<String, String> tableAttr = new HashMap<>();
        tableAttr.put("table", table);
        tableAttr.put("field", field);
        return tableAttr;
    }
}