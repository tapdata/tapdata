package com.tapdata.tm.task.service.batchin.dto.project.content;

import lombok.Data;

import java.util.Map;

@Data
public class ContentMapping {
    protected Object settings;
    protected Map<String, Map<String, Object>> fields;
    protected Map<String, Object> calculatedFields;
    protected String collectionId;
    protected String table;

    public String[] splitTable() {
        if(null == table || table.length() == 0) return new String[0];
        return table.split("\\.");
    }
}
