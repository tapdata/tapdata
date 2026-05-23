package com.tapdata.tm.duckdb.controller;

import com.tapdata.tm.duckdb.dto.DuckDbSqlTestRequest;
import com.tapdata.tm.duckdb.dto.DuckDbSqlTestResponse;
import com.tapdata.tm.duckdb.dto.DuckDbTableSchemaDto;
import com.tapdata.tm.duckdb.dto.DuckDbDeadLetterDto;
import com.tapdata.tm.duckdb.service.DuckDbSqlNodeService;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
@RequestMapping("/api/duckdb")
public class DuckDbSqlNodeController {
    
    private final DuckDbSqlNodeService service;
    
    public DuckDbSqlNodeController(DuckDbSqlNodeService service) {
        this.service = service;
    }
    
    @PostMapping("/test")
    public DuckDbSqlTestResponse test(@RequestBody DuckDbSqlTestRequest request) {
        return service.testSql(request);
    }
    
    @GetMapping("/table-schema")
    public DuckDbTableSchemaDto tableSchema(@RequestParam String tableName) {
        return service.getTableSchema(tableName);
    }
    
    @GetMapping("/dead-letter")
    public List<DuckDbDeadLetterDto> deadLetter(@RequestParam String nodeId, @RequestParam(defaultValue = "50") int limit) {
        return service.listDeadLetters(nodeId, limit);
    }
}
