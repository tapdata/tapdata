package io.tapdata.services;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class DuckDbTableSchemaDto implements Serializable {
    private String tableName;
    private Map<String, String> fields;
    private boolean success;
    private String errorMessage;
    private String errorType;
}
