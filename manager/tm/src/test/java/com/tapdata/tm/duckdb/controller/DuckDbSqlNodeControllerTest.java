package com.tapdata.tm.duckdb.controller;

import com.tapdata.tm.duckdb.dto.DuckDbSqlTestRequest;
import com.tapdata.tm.duckdb.dto.DuckDbSqlTestResponse;
import com.tapdata.tm.duckdb.service.DuckDbSqlNodeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DuckDbSqlNodeControllerTest {
    
    private DuckDbSqlNodeController controller;
    private DuckDbSqlNodeService service;
    
    @BeforeEach
    void setUp() {
        service = mock(DuckDbSqlNodeService.class);
        controller = new DuckDbSqlNodeController(service);
    }
    
    @Test
    void testSqlEndpointReturnsRowsAndTiming() {
        DuckDbSqlTestRequest request = new DuckDbSqlTestRequest();
        request.setSql("SELECT 1 as id");
        
        DuckDbSqlTestResponse expectedResponse = new DuckDbSqlTestResponse();
        expectedResponse.setSuccess(true);
        expectedResponse.setRows(List.of(Map.of("id", 1)));
        expectedResponse.setRowCount(1);
        expectedResponse.setExecutionTimeMs(12L);
        
        when(service.testSql(request)).thenReturn(expectedResponse);
        
        DuckDbSqlTestResponse actualResponse = controller.test(request);
        
        assertEquals(true, actualResponse.isSuccess());
        assertEquals(1, actualResponse.getRowCount());
        assertEquals(List.of(Map.of("id", 1)), actualResponse.getRows());
        assertEquals(12L, actualResponse.getExecutionTimeMs());
    }
}

