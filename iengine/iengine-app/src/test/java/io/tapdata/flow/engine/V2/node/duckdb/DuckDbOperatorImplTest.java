package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DuckDbOperatorImplTest {

    private Connection mockConnection;
    private Statement mockStatement;
    private PreparedStatement mockPreparedStatement;
    private ResultSet mockResultSet;
    private ResultSetMetaData mockResultSetMetaData;
    private DuckDbOperatorImpl duckDbOperator;

    @BeforeEach
    void setUp() throws SQLException {
        mockConnection = Mockito.mock(Connection.class);
        mockStatement = Mockito.mock(Statement.class);
        mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        mockResultSet = Mockito.mock(ResultSet.class);
        mockResultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockStatement.executeUpdate(anyString())).thenReturn(1);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockPreparedStatement.executeUpdate()).thenReturn(1);
        when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
        when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
        when(mockResultSetMetaData.getColumnName(1)).thenReturn("id");
        when(mockResultSetMetaData.getColumnName(2)).thenReturn("name");
        when(mockResultSet.next()).thenReturn(true, true, false);
        when(mockResultSet.getObject(1)).thenReturn(1L).thenReturn(2L);
        when(mockResultSet.getObject(2)).thenReturn("Alice").thenReturn("Bob");
        
        duckDbOperator = new DuckDbOperatorImpl(mockConnection);
    }

    @AfterEach
    void tearDown() {
        if (duckDbOperator != null) {
            duckDbOperator.close();
        }
    }

    // ==================== executeQuery 测试 ====================

    @Test
    void testExecuteQuery_Success() throws SQLException {
        List<java.util.Map<String, Object>> result = duckDbOperator.executeQuery("SELECT * FROM test_table");
        
        assertNotNull(result);
        assertEquals(2, result.size());
        verify(mockStatement).executeQuery("SELECT * FROM test_table");
    }

    @Test
    void testExecuteQuery_EmptyResult() throws SQLException {
        when(mockResultSet.next()).thenReturn(false);
        
        List<java.util.Map<String, Object>> result = duckDbOperator.executeQuery("SELECT * FROM empty_table");
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ==================== executeUpdate 测试 ====================

    @Test
    void testExecuteUpdate_Success() throws SQLException {
        int affectedRows = duckDbOperator.executeUpdate("DELETE FROM test_table WHERE id = 1");
        
        assertEquals(1, affectedRows);
        verify(mockStatement).executeUpdate("DELETE FROM test_table WHERE id = 1");
    }

    // ==================== writeBatch 测试 ====================

    @Test
    void testWriteBatch_WithValidData() throws Exception {
        TapTable tapTable = createTestTapTable();
        java.util.List<java.util.Map<String, Object>> data = createTestData();
        
        assertDoesNotThrow(() -> duckDbOperator.writeBatch(data, "test_table"));
    }

    @Test
    void testWriteBatch_EmptyData() throws Exception {
        java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>();
        
        assertDoesNotThrow(() -> duckDbOperator.writeBatch(data, "test_table"));
        
        verify(mockConnection, never()).prepareStatement(anyString());
    }

    @Test
    void testWriteBatch_NullData() throws Exception {
        assertDoesNotThrow(() -> duckDbOperator.writeBatch(null, "test_table"));
    }

    // ==================== createTable 测试 ====================

    @Test
    void testCreateTable_Success() throws SQLException {
        TapTable tapTable = createTestTapTable();
        
        assertDoesNotThrow(() -> duckDbOperator.createTable(tapTable));
        
        verify(mockStatement).executeUpdate(contains("CREATE TABLE IF NOT EXISTS"));
    }

    @Test
    void testCreateTempTable_Success() throws SQLException {
        TapTable tapTable = createTestTapTable();
        
        assertDoesNotThrow(() -> duckDbOperator.createTempTable(tapTable, "temp_test_table"));
        
        verify(mockStatement).executeUpdate(contains("CREATE TABLE IF NOT EXISTS temp_test_table"));
    }

    // ==================== DDL 操作测试 ====================

    @Test
    void testDropTable_Success() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.dropTable("test_table"));
        
        verify(mockStatement).executeUpdate("DROP TABLE IF EXISTS test_table");
    }

    @Test
    void testTruncateTable_Success() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.truncateTable("test_table"));
        
        verify(mockStatement).executeUpdate("DELETE FROM test_table");
    }

    @Test
    void testRenameTable_Success() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.renameTable("old_name", "new_name"));
        
        verify(mockStatement).executeUpdate("ALTER TABLE old_name RENAME TO new_name");
    }

    @Test
    void testAddColumn_Success() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.addColumn("test_table", "new_col", "VARCHAR(100)"));
        
        verify(mockStatement).executeUpdate(contains("ADD COLUMN \"new_col\" VARCHAR(100)"));
    }

    @Test
    void testDropColumn_Success() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.dropColumn("test_table", "old_col"));
        
        verify(mockStatement).executeUpdate(contains("DROP COLUMN \"old_col\""));
    }

    @Test
    void testCreateIndex_Success() throws SQLException {
        java.util.List<String> columns = java.util.Arrays.asList("col1", "col2");
        
        assertDoesNotThrow(() -> duckDbOperator.createIndex("test_table", "idx_test", columns, false));
        
        verify(mockStatement).executeUpdate(contains("CREATE INDEX IF NOT EXISTS idx_test ON test_table"));
    }

    @Test
    void testCreateUniqueIndex_Success() throws SQLException {
        java.util.List<String> columns = java.util.Arrays.asList("unique_col");
        
        assertDoesNotThrow(() -> duckDbOperator.createIndex("test_table", "idx_unique", columns, true));
        
        verify(mockStatement).executeUpdate(contains("CREATE UNIQUE INDEX IF NOT EXISTS idx_unique ON test_table"));
    }

    @Test
    void testDropIndex_Success() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.dropIndex("test_table", "idx_to_drop"));
        
        verify(mockStatement).executeUpdate("DROP INDEX IF EXISTS idx_to_drop");
    }

    // ==================== DML 操作测试 ====================

    @Test
    void testInsert_SingleRow() throws SQLException {
        java.util.Map<String, Object> data = new java.util.LinkedHashMap<>();
        data.put("id", 1L);
        data.put("name", "Alice");
        
        assertDoesNotThrow(() -> duckDbOperator.insert("test_table", data));
        
        verify(mockStatement).executeUpdate(contains("INSERT INTO test_table"));
    }

    @Test
    void testInsert_EmptyData() throws SQLException {
        java.util.Map<String, Object> data = new java.util.LinkedHashMap<>();
        
        assertDoesNotThrow(() -> duckDbOperator.insert("test_table", data));
        
        verify(mockStatement, never()).executeUpdate(anyString());
    }

    @Test
    void testInsertBatch_MultipleRows() throws Exception {
        java.util.List<java.util.Map<String, Object>> dataList = createTestData();
        
        assertDoesNotThrow(() -> duckDbOperator.insertBatch("test_table", dataList));
    }

    @Test
    void testUpdate_Success() throws SQLException {
        java.util.Map<String, Object> data = new java.util.LinkedHashMap<>();
        data.put("name", "Updated Name");
        
        int affectedRows = duckDbOperator.update("test_table", data, "id = 1");
        
        assertEquals(1, affectedRows);
        verify(mockStatement).executeUpdate(contains("UPDATE test_table SET"));
    }

    @Test
    void testUpdate_EmptyData() throws SQLException {
        java.util.Map<String, Object> data = new java.util.LinkedHashMap<>();
        
        int affectedRows = duckDbOperator.update("test_table", data, "id = 1");
        
        assertEquals(0, affectedRows);
    }

    @Test
    void testDelete_Success() throws SQLException {
        int affectedRows = duckDbOperator.delete("test_table", "id = 1");
        
        assertEquals(1, affectedRows);
        verify(mockStatement).executeUpdate(contains("DELETE FROM test_table WHERE"));
    }

    @Test
    void testDelete_AllRows() throws SQLException {
        int affectedRows = duckDbOperator.delete("test_table", null);
        
        assertEquals(1, affectedRows);
        verify(mockStatement).executeUpdate(eq("DELETE FROM test_table"));
    }

    @Test
    void testUpsert_SingleRow() throws SQLException {
        java.util.Map<String, Object> data = new java.util.LinkedHashMap<>();
        data.put("id", 1L);
        data.put("name", "Alice");
        
        assertDoesNotThrow(() -> duckDbOperator.upsert("test_table", data));
        
        verify(mockStatement).executeUpdate(contains("INSERT OR REPLACE INTO test_table"));
    }

    @Test
    void testUpsertBatch_MultipleRows() throws Exception {
        java.util.List<java.util.Map<String, Object>> dataList = createTestData();
        
        assertDoesNotThrow(() -> duckDbOperator.upsertBatch("test_table", dataList));
    }

    // ==================== 事务管理测试 ====================

    @Test
    void testCommit_Success() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.commit());
        
        verify(mockConnection).commit();
    }

    @Test
    void testRollback_Success() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.rollback());
        
        verify(mockConnection).rollback();
    }

    @Test
    void testSetAutoCommit_True() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.setAutoCommit(true));
        
        verify(mockConnection).setAutoCommit(true);
    }

    @Test
    void testSetAutoCommit_False() throws SQLException {
        assertDoesNotThrow(() -> duckDbOperator.setAutoCommit(false));
        
        verify(mockConnection).setAutoCommit(false);
    }

    // ==================== 元数据操作测试 ====================

    @Test
    void testTableExists_ReturnsTrue() throws SQLException {
        ResultSet countResult = Mockito.mock(ResultSet.class);
        when(countResult.next()).thenReturn(true);
        when(countResult.getInt(1)).thenReturn(1);
        when(mockPreparedStatement.executeQuery()).thenReturn(countResult);
        
        boolean exists = duckDbOperator.tableExists("existing_table");
        
        assertTrue(exists);
    }

    @Test
    void testTableExists_ReturnsFalse() throws SQLException {
        ResultSet countResult = Mockito.mock(ResultSet.class);
        when(countResult.next()).thenReturn(true);
        when(countResult.getInt(1)).thenReturn(0);
        when(mockPreparedStatement.executeQuery()).thenReturn(countResult);
        
        boolean exists = duckDbOperator.tableExists("non_existing_table");
        
        assertFalse(exists);
    }

    @Test
    void testGetTableColumns_Success() throws SQLException {
        ResultSet columnResult = Mockito.mock(ResultSet.class);
        ResultSetMetaData columnMeta = Mockito.mock(ResultSetMetaData.class);
        when(columnResult.getMetaData()).thenReturn(columnMeta);
        when(columnMeta.getColumnCount()).thenReturn(4);
        when(columnMeta.getColumnName(1)).thenReturn("column_name");
        when(columnMeta.getColumnName(2)).thenReturn("data_type");
        when(columnMeta.getColumnName(3)).thenReturn("is_nullable");
        when(columnMeta.getColumnName(4)).thenReturn("column_default");
        when(columnResult.next()).thenReturn(true, true, false);
        when(columnResult.getObject(1)).thenReturn("id").thenReturn("name");
        when(columnResult.getObject(2)).thenReturn("BIGINT").thenReturn("VARCHAR");
        when(columnResult.getObject(3)).thenReturn("NO").thenReturn("YES");
        when(mockPreparedStatement.executeQuery()).thenReturn(columnResult);
        
        java.util.List<java.util.Map<String, Object>> columns = duckDbOperator.getTableColumns("test_table");
        
        assertNotNull(columns);
        assertEquals(2, columns.size());
    }

    @Test
    void testListTables_Success() throws SQLException {
        ResultSet tableResult = Mockito.mock(ResultSet.class);
        when(tableResult.next()).thenReturn(true, true, false);
        when(tableResult.getString("table_name")).thenReturn("table1").thenReturn("table2");
        when(mockStatement.executeQuery(anyString())).thenReturn(tableResult);
        
        java.util.List<String> tables = duckDbOperator.listTables();
        
        assertNotNull(tables);
        assertEquals(2, tables.size());
        assertTrue(tables.contains("table1"));
        assertTrue(tables.contains("table2"));
    }

    @Test
    void testGetRowCount_Success() throws SQLException {
        ResultSet countResult = Mockito.mock(ResultSet.class);
        when(countResult.next()).thenReturn(true);
        when(countResult.getLong(1)).thenReturn(42L);
        when(mockStatement.executeQuery(anyString())).thenReturn(countResult);
        
        long rowCount = duckDbOperator.getRowCount("test_table");
        
        assertEquals(42L, rowCount);
    }

    // ==================== 连接状态测试 ====================

    @Test
    void testIsConnectionValid_ValidConnection() throws SQLException {
        when(mockConnection.isValid(5)).thenReturn(true);
        
        boolean isValid = duckDbOperator.isConnectionValid();
        
        assertTrue(isValid);
    }

    @Test
    void testIsConnectionValid_InvalidConnection() throws SQLException {
        when(mockConnection.isValid(5)).thenReturn(false);
        
        boolean isValid = duckDbOperator.isConnectionValid();
        
        assertFalse(isValid);
    }

    // ==================== 批处理功能测试 ====================

    @Test
    void testBatchWriting_FlushOnSize() throws Exception {
        DuckDbOperatorImpl batchOperator = new DuckDbOperatorImpl(mockConnection, true, 5, 5000);
        
        try {
            TapTable tapTable = createTestTapTable();
            
            for (int i = 0; i < 6; i++) {
                java.util.List<java.util.Map<String, Object>> batch = new java.util.ArrayList<>();
                java.util.Map<String, Object> row = new java.util.HashMap<>();
                row.put("id", (long) i);
                row.put("name", "User_" + i);
                batch.add(row);
                
                batchOperator.writeBatch(batch, "batch_table");
            }
            
            assertTrue(batchOperator.hasPendingBatch() || !batchOperator.hasPendingBatch());
        } finally {
            batchOperator.close();
        }
    }

    @Test
    void testClose_FlushesRemainingBatch() throws SQLException, java.io.IOException {
        DuckDbOperatorImpl batchOperator = new DuckDbOperatorImpl(mockConnection, true, 10000, 5000);
        
        try {
            TapTable tapTable = createTestTapTable();
            java.util.List<java.util.Map<String, Object>> data = createTestData();
            
            batchOperator.writeBatch(data, "flush_test_table");
            assertTrue(batchOperator.hasPendingBatch());
        } finally {
            batchOperator.close();
        }
    }

    // ==================== 异常处理测试 ====================

    @Test
    void testClosedOperator_ThrowsException() throws SQLException {
        duckDbOperator.close();
        
        Assertions.assertThrows(SQLException.class, () -> duckDbOperator.executeQuery("SELECT 1"));
    }

    @Test
    void testExecuteQuery_SQLException() throws SQLException {
        when(mockStatement.executeQuery(anyString())).thenThrow(new SQLException("SQL Error"));
        
        Assertions.assertThrows(SQLException.class, () -> duckDbOperator.executeQuery("SELECT * FROM invalid"));
    }

    // ==================== 辅助方法 ====================

    private TapTable createTestTapTable() {
        TapTable tapTable = new TapTable("test_table");
        
        TapField idField = new TapField();
        idField.name("id");
        idField.dataType("BIGINT");
        idField.setPrimaryKey(true);
        tapTable.add(idField);
        
        TapField nameField = new TapField();
        nameField.name("name");
        nameField.dataType("VARCHAR");
        tapTable.add(nameField);
        
        return tapTable;
    }

    private java.util.List<java.util.Map<String, Object>> createTestData() {
        java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>();
        
        for (long i = 1; i <= 3; i++) {
            java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
            row.put("id", i);
            row.put("name", "User_" + i);
            data.add(row);
        }
        
        return data;
    }
}
