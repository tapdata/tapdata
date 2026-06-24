package com.tapdata.tm.commons.dag.process.duck;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Data
public class JoinField {
    public static final String TABLE = "table";
    public static final String FIELD = "field";

    private String table;

    private String field;

    public Map<String, String> toMap() {
        Map<String, String> tableAttr = new HashMap<>();
        tableAttr.put(TABLE, table);
        tableAttr.put(FIELD, field);
        return tableAttr;
    }

    public static String fieldsOf(Object info, String table) {
        if (!(info instanceof Map<?, ?> iMap)) {
            return null;
        }
        if (Objects.equals(iMap.get(TABLE), table)) {
            return String.valueOf(iMap.get(FIELD));
        }
        return null;
    }
}