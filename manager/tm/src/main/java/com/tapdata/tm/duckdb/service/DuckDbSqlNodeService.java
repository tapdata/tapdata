package com.tapdata.tm.duckdb.service;

import com.tapdata.tm.duckdb.dto.DuckDbSqlTestRequest;
import com.tapdata.tm.duckdb.dto.DuckDbSqlTestResponse;
import com.tapdata.tm.duckdb.dto.DuckDbTableSchemaDto;
import com.tapdata.tm.duckdb.dto.DuckDbDeadLetterDto;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class DuckDbSqlNodeService {
    
    public DuckDbSqlTestResponse testSql(DuckDbSqlTestRequest request) {
        DuckDbSqlTestResponse response = new DuckDbSqlTestResponse();
        response.setSuccess(true);
        response.setRowCount(0);
        response.setExecutionTimeMs(0);
        return response;
    }
    
    public DuckDbTableSchemaDto getTableSchema(String tableName) {
        DuckDbTableSchemaDto dto = new DuckDbTableSchemaDto();
        dto.setTableName(tableName);
        return dto;
    }
    
    public List<DuckDbDeadLetterDto> listDeadLetters(String nodeId, int limit) {
        return List.of();
    }
}
