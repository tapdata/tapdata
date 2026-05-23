package com.tapdata.tm.duckdb.dto;

import lombok.Getter;
import lombok.Setter;
import java.util.Map;

@Getter
@Setter
public class DuckDbTableSchemaDto {
    private String tableName;
    private Map<String, String> fields;
}
