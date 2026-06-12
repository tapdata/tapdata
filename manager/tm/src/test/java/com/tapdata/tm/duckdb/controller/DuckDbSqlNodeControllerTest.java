package com.tapdata.tm.duckdb.controller;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.duckdb.dto.DuckDbSqlTestRequest;
import com.tapdata.tm.duckdb.dto.DuckDbSqlTestResponse;
import com.tapdata.tm.duckdb.dto.DuckDbTableSchemaDto;
import com.tapdata.tm.duckdb.dto.DuckDbDeadLetterDto;
import com.tapdata.tm.duckdb.service.DuckDbSqlNodeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.List;
import java.util.Map;
import java.time.LocalDateTime;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DuckDbSqlNodeControllerTest {
    
    @Mock
    private DuckDbSqlNodeService service;
    
    @InjectMocks
    private DuckDbSqlNodeController controller;
    
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
        
        ResponseMessage<DuckDbSqlTestResponse> actualResponse = controller.test(request);
        
        assertNotNull(actualResponse);
        assertEquals("ok", actualResponse.getCode());
        assertNotNull(actualResponse.getData());
        assertTrue(actualResponse.getData().isSuccess());
        assertEquals(1, actualResponse.getData().getRowCount());
        assertEquals(List.of(Map.of("id", 1)), actualResponse.getData().getRows());
        assertEquals(12L, actualResponse.getData().getExecutionTimeMs());
    }
    
    @Test
    void testTableSchemaEndpointReturnsSchemaInfo() {
        String tableName = "test_table";
        
        DuckDbTableSchemaDto expectedDto = new DuckDbTableSchemaDto();
        expectedDto.setTableName(tableName);
        expectedDto.setFields(Map.of("id", "INTEGER(32) NOT NULL", "name", "VARCHAR(255) NULLABLE"));
        
        when(service.getTableSchema(tableName)).thenReturn(expectedDto);
        
        ResponseMessage<DuckDbTableSchemaDto> actualResponse = controller.tableSchema(tableName);
        
        assertNotNull(actualResponse);
        assertEquals("ok", actualResponse.getCode());
        assertNotNull(actualResponse.getData());
        assertEquals(tableName, actualResponse.getData().getTableName());
        assertNotNull(actualResponse.getData().getFields());
        assertEquals(2, actualResponse.getData().getFields().size());
    }
    
    @Test
    void testDeadLetterEndpointReturnsRecords() {
        String nodeId = "test_node";
        int limit = 10;
        
        DuckDbDeadLetterDto deadLetter = new DuckDbDeadLetterDto();
        deadLetter.setSourceId(nodeId);
        deadLetter.setTableId(nodeId);
        deadLetter.setTargetTableName("test_table");
        deadLetter.setPayload("{\"test\": \"data\"}");
        deadLetter.setErrorMessage("Test error");
        deadLetter.setErrorClass("TestException");
        deadLetter.setCreatedAt(LocalDateTime.now());
        
        when(service.listDeadLetters(nodeId, limit)).thenReturn(List.of(deadLetter));
        
        ResponseMessage<List<DuckDbDeadLetterDto>> actualResponse = controller.deadLetter(nodeId, limit);
        
        assertNotNull(actualResponse);
        assertEquals("ok", actualResponse.getCode());
        assertNotNull(actualResponse.getData());
        assertEquals(1, actualResponse.getData().size());
        assertEquals(nodeId, actualResponse.getData().get(0).getSourceId());
    }
    
    @Test
    void testSqlEndpointHandlesError() {
        DuckDbSqlTestRequest request = new DuckDbSqlTestRequest();
        request.setSql("INVALID SQL");
        
        DuckDbSqlTestResponse expectedResponse = new DuckDbSqlTestResponse();
        expectedResponse.setSuccess(false);
        expectedResponse.setErrorMessage("Syntax error");
        
        when(service.testSql(request)).thenReturn(expectedResponse);
        
        ResponseMessage<DuckDbSqlTestResponse> actualResponse = controller.test(request);
        
        assertNotNull(actualResponse);
        assertNotNull(actualResponse.getData());
        assertFalse(actualResponse.getData().isSuccess());
        assertEquals("Syntax error", actualResponse.getData().getErrorMessage());
    }
}

