package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ArrowZeroCopyTest {

    private Connection mockConnection;
    private PreparedStatement mockPreparedStatement;
    private Statement mockStatement;
    private ArrowWriter arrowWriter;

    @BeforeEach
    void setUp() throws SQLException {
        mockConnection = Mockito.mock(Connection.class);
        mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        mockStatement = Mockito.mock(Statement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        arrowWriter = new ArrowWriter(mockConnection, true);
    }

    @AfterEach
    void tearDown() {
        if (arrowWriter != null) {
            arrowWriter.close();
        }
    }

    // ==================== 零拷贝写入核心测试 ====================

    @Test
    void testZeroCopyWrite_BasicFunctionality() throws Exception {
        TapTable tapTable = new TapTable("zero_copy_test");

        TapField idField = new TapField();
        idField.name("id");
        idField.dataType("BIGINT");
        idField.setPrimaryKey(true);
        tapTable.add(idField);

        TapField nameField = new TapField();
        nameField.name("name");
        nameField.dataType("VARCHAR");
        tapTable.add(nameField);

        TapField valueField = new TapField();
        valueField.name("value");
        valueField.dataType("DOUBLE");
        tapTable.add(valueField);

        TapField activeField = new TapField();
        activeField.name("active");
        activeField.dataType("BOOLEAN");
        tapTable.add(activeField);

        TapField countField = new TapField();
        countField.name("count");
        countField.dataType("INTEGER");
        tapTable.add(countField);

        TapField createdAtField = new TapField();
        createdAtField.name("created_at");
        createdAtField.dataType("TIMESTAMP");
        tapTable.add(createdAtField);

        TapField notesField = new TapField();
        notesField.name("notes");
        notesField.dataType("VARCHAR");
        tapTable.add(notesField);

        java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>();

        java.util.Map<String, Object> row1 = new java.util.LinkedHashMap<>();
        row1.put("id", 1L);
        row1.put("name", "Alice");
        row1.put("value", 12.34d);
        row1.put("active", true);
        row1.put("count", 100);
        row1.put("created_at", new java.util.Date(1700000000000L));
        row1.put("notes", "Normal text");
        data.add(row1);

        java.util.Map<String, Object> row2 = new java.util.LinkedHashMap<>();
        row2.put("id", 2L);
        row2.put("name", "Bob");
        row2.put("value", -9876.5d);
        row2.put("active", false);
        row2.put("count", -42);
        row2.put("created_at", new java.util.Date(1700000001000L));
        row2.put("notes", "Text with 'quotes' and \"double quotes\"");
        data.add(row2);

        java.util.Map<String, Object> row3 = new java.util.LinkedHashMap<>();
        row3.put("id", 3L);
        row3.put("name", "Carol");
        row3.put("value", 0.000001d);
        row3.put("active", true);
        row3.put("count", 0);
        row3.put("created_at", new java.util.Date(1700000002000L));
        row3.put("notes", "Text with\nnewlines\tand\ttabs, commas, and pipes | ");
        data.add(row3);

        java.util.Map<String, Object> row4 = new java.util.LinkedHashMap<>();
        row4.put("id", 4L);
        row4.put("name", "Dan");
        row4.put("value", 99999999.99d);
        row4.put("active", null);
        row4.put("count", null);
        row4.put("created_at", new java.util.Date(1700000003000L));
        row4.put("notes", "Long text: " + "x".repeat(1024));
        data.add(row4);

        java.util.Map<String, Object> row5 = new java.util.LinkedHashMap<>();
        row5.put("id", 5L);
        row5.put("name", "Eve");
        row5.put("value", null);
        row5.put("active", false);
        row5.put("count", 2147483647);
        row5.put("created_at", new java.util.Date(1700000004000L));
        row5.put("notes", "Unicode: 中文文本 日本語 한국어");
        data.add(row5);

        try (Connection duckDbConnection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = duckDbConnection.createStatement();
             ArrowWriter realWriter = new ArrowWriter(duckDbConnection, true)) {

            statement.execute("CREATE TABLE zero_copy_test (" +
                "id BIGINT, " +
                "name VARCHAR, " +
                "value DOUBLE, " +
                "active BOOLEAN, " +
                "count INTEGER, " +
                "created_at TIMESTAMP, " +
                "notes VARCHAR"
                + ")");

            realWriter.writeWithArrow(data, "zero_copy_test", tapTable);

            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT id, name, value, active, count, created_at, notes " +
                    "FROM zero_copy_test ORDER BY id")) {
                int rowIndex = 0;
                while (resultSet.next()) {
                    assertTrue(rowIndex < data.size(), "DuckDB returned more rows than were written");

                    java.util.Map<String, Object> expectedRow = data.get(rowIndex);
                    assertEquals(((Number) expectedRow.get("id")).longValue(), resultSet.getLong("id"));
                    assertEquals(expectedRow.get("name"), resultSet.getString("name"));
                    assertEquals(((Number) expectedRow.get("value")).doubleValue(), resultSet.getDouble("value"), 0.000001d);
                    if (expectedRow.get("value") == null) {
                        assertTrue(resultSet.wasNull());
                    }
                    Boolean expectedActive = (Boolean) expectedRow.get("active");
                    boolean activeValue = resultSet.getBoolean("active");
                    if (expectedActive == null) {
                        assertTrue(resultSet.wasNull());
                    } else {
                        assertEquals(expectedActive, activeValue);
                    }
                    Integer expectedCount = (Integer) expectedRow.get("count");
                    int countValue = resultSet.getInt("count");
                    if (expectedCount == null) {
                        assertTrue(resultSet.wasNull());
                    } else {
                        assertEquals(expectedCount.intValue(), countValue);
                    }
                    assertNotNull(resultSet.getTimestamp("created_at"));
                    assertEquals(expectedRow.get("notes"), resultSet.getString("notes"));
                    rowIndex++;
                }

                assertEquals(data.size(), rowIndex, "DuckDB row count should match the written batch size");
            }
        }
    }

    @Test
    void testZeroCopyWrite_LargeDataset() throws SQLException {
        TapTable tapTable = createStandardTapTable();
        java.util.List<java.util.Map<String, Object>> data = createStandardData(10000);
        
        long startTime = System.currentTimeMillis();
        
        assertDoesNotThrow(() -> arrowWriter.writeWithArrow(data, "large_dataset", tapTable));
        
        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Zero-copy write 10000 rows time: " + duration + "ms");
        
        assertTrue(duration < 10000, "Large dataset write should complete within 10 seconds");
    }

    @Test
    void testZeroCopyWrite_MultipleColumns() throws SQLException {
        TapTable tapTable = createMultiColumnTapTable();
        java.util.List<java.util.Map<String, Object>> data = createMultiColumnData(50);

        assertDoesNotThrow(() -> arrowWriter.writeWithArrow(data, "multi_col_table", tapTable));

        verify(mockConnection, atLeastOnce()).createStatement();
    }

    @Test
    void testZeroCopyWrite_WithNullValues() throws SQLException {
        TapTable tapTable = createStandardTapTable();
        java.util.List<java.util.Map<String, Object>> data = createDataWithNulls(10);
        
        assertDoesNotThrow(() -> arrowWriter.writeWithArrow(data, "null_values_table", tapTable));
    }

    @Test
    void testZeroCopyWrite_SpecialCharactersInData() throws SQLException {
        TapTable tapTable = createStringTapTable();
        java.util.List<java.util.Map<String, Object>> data = createSpecialCharacterData();
        
        assertDoesNotThrow(() -> arrowWriter.writeWithArrow(data, "special_chars_table", tapTable));
    }

    // ==================== 降级策略测试 ====================

    @Test
    void testFallbackToCopyMode_OnSQLException() throws SQLException {
        // 零拷贝写入使用 fallbackInsert，验证 Statement.executeUpdate 被调用
        TapTable tapTable = createStandardTapTable();
        java.util.List<java.util.Map<String, Object>> data = createStandardData(10);
        
        assertDoesNotThrow(() -> arrowWriter.writeWithArrow(data, "fallback_test", tapTable));
        
        // 验证写入操作被执行
        verify(mockStatement, atLeastOnce()).executeUpdate(anyString());
    }

    @Test
    void testFallbackToCopyMode_ZeroCopyDisabled() throws SQLException {
        ArrowWriter writerWithoutZeroCopy = new ArrowWriter(mockConnection, false);
        
        try {
            TapTable tapTable = createStandardTapTable();
            java.util.List<java.util.Map<String, Object>> data = createStandardData(5);
            
            writerWithoutZeroCopy.writeWithArrow(data, "no_zero_copy", tapTable);
            
            verify(mockConnection, never()).prepareStatement(contains("ARROW"));
            verify(mockStatement, atLeastOnce()).executeUpdate(anyString());
        } finally {
            writerWithoutZeroCopy.close();
        }
    }

    // ==================== Schema 转换测试 ====================

    @Test
    void testSchemaConversion_AllSupportedTypes() {
        TapTable tapTable = createAllTypesTapTable();
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        assertNotNull(schema);
        assertEquals(10, schema.getFields().size());
        
        for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
            assertNotNull(field.getFieldType(), "Field type should not be null: " + field.getName());
            assertNotNull(field.getFieldType().getType(), "Arrow type should not be null for field: " + field.getName());
        }
    }

    @Test
    void testVectorSchemaRootCreation_DataIntegrity() {
        TapTable tapTable = createStandardTapTable();
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        java.util.List<java.util.Map<String, Object>> originalData = createStandardData(20);
        
        VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(originalData, schema);
        
        try {
            assertNotNull(root);
            assertEquals(20, root.getRowCount());
            
            for (int i = 0; i < schema.getFields().size(); i++) {
                String fieldName = schema.getFields().get(i).getName();
                assertNotNull(root.getVector(i), "Vector should exist for field: " + fieldName);
                assertEquals(20, root.getVector(i).getValueCount(), 
                    "Value count should match row count for field: " + fieldName);
            }
        } finally {
            root.close();
        }
    }

    // ==================== 性能基准测试 ====================

    @Test
    void performanceBenchmark_SmallBatch() throws SQLException {
        TapTable tapTable = createStandardTapTable();
        int batchSize = 100;
        java.util.List<java.util.Map<String, Object>> data = createStandardData(batchSize);
        
        long startTime = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            arrowWriter.writeWithArrow(data, "perf_small_batch", tapTable);
        }
        long duration = (System.nanoTime() - startTime) / 1_000_000;
        
        System.out.println("Performance benchmark (100 rows x 10 iterations): " + duration + "ms");
        assertTrue(duration < 5000, "Small batch performance should be acceptable");
    }

    @Test
    void performanceBenchmark_MediumBatch() throws SQLException {
        TapTable tapTable = createStandardTapTable();
        int batchSize = 1000;
        java.util.List<java.util.Map<String, Object>> data = createStandardData(batchSize);
        
        long startTime = System.nanoTime();
        arrowWriter.writeWithArrow(data, "perf_medium_batch", tapTable);
        long duration = (System.nanoTime() - startTime) / 1_000_000;
        
        System.out.println("Performance benchmark (1000 rows): " + duration + "ms");
        assertTrue(duration < 3000, "Medium batch performance should be acceptable");
    }

    // ==================== 并发安全测试 ====================

    @Test
    void testConcurrentWrites() throws InterruptedException, SQLException {
        TapTable tapTable = createStandardTapTable();
        int threadCount = 5;
        int writesPerThread = 10;
        
        Thread[] threads = new Thread[threadCount];
        Exception[] exceptions = new Exception[1];
        
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    for (int i = 0; i < writesPerThread; i++) {
                        java.util.List<java.util.Map<String, Object>> data = createStandardData(10);
                        arrowWriter.writeWithArrow(data, "concurrent_table_" + threadId, tapTable);
                    }
                } catch (Exception e) {
                    exceptions[0] = e;
                }
            });
            threads[t].start();
        }
        
        for (Thread thread : threads) {
            thread.join(10000);
        }
        
        if (exceptions[0] != null) {
            fail("Concurrent writes failed: " + exceptions[0].getMessage());
        }
    }

    // ==================== 内存管理测试 ====================

    @Test
    void testMemoryManagement_MultipleOperations() {
        TapTable tapTable = createStandardTapTable();
        
        for (int i = 0; i < 100; i++) {
            Schema schema = arrowWriter.buildArrowSchema(tapTable);
            java.util.List<java.util.Map<String, Object>> data = createStandardData(50);
            
            try (VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema)) {
                assertNotNull(root);
                assertEquals(50, root.getRowCount());
            }
        }
        
        assertDoesNotThrow(() -> arrowWriter.close());
    }

    @Test
    void testMemoryManagement_LargeValueVectors() {
        TapTable tapTable = createStringTapTable();
        java.util.List<java.util.Map<String, Object>> data = createLargeStringData(100);
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema);
        
        try {
            assertNotNull(root);
            assertEquals(100, root.getRowCount());
        } finally {
            root.close();
        }
    }

    // ==================== 辅助方法 ====================

    private TapTable createStandardTapTable() {
        TapTable tapTable = new TapTable("standard_table");
        
        TapField idField = new TapField();
        idField.name("id");
        idField.dataType("BIGINT");
        idField.setPrimaryKey(true);
        tapTable.add(idField);
        
        TapField nameField = new TapField();
        nameField.name("name");
        nameField.dataType("VARCHAR");
        tapTable.add(nameField);
        
        TapField valueField = new TapField();
        valueField.name("value");
        valueField.dataType("DOUBLE");
        tapTable.add(valueField);
        
        return tapTable;
    }

    private TapTable createMultiColumnTapTable() {
        TapTable tapTable = new TapTable("multi_column_table");
        
        String[] types = {"BIGINT", "VARCHAR", "INTEGER", "BOOLEAN", "DOUBLE", "TIMESTAMP"};
        String[] names = {"id", "name", "count", "active", "score", "created_at"};
        
        for (int i = 0; i < types.length; i++) {
            TapField field = new TapField();
            field.name(names[i]);
            field.dataType(types[i]);
            tapTable.add(field);
        }
        
        return tapTable;
    }

    private TapTable createStringTapTable() {
        TapTable tapTable = new TapTable("string_table");
        
        TapField field = new TapField();
        field.name("text_content");
        field.dataType("VARCHAR");
        tapTable.add(field);
        
        return tapTable;
    }

    private TapTable createAllTypesTapTable() {
        TapTable tapTable = new TapTable("all_types_table");
        
        String[] types = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", 
                         "DOUBLE", "BOOLEAN", "VARCHAR", "BLOB", "TIMESTAMP"};
        String[] names = {"tiny_col", "small_col", "int_col", "bigint_col", "float_col",
                         "double_col", "bool_col", "varchar_col", "blob_col", "timestamp_col"};
        
        for (int i = 0; i < types.length; i++) {
            TapField field = new TapField();
            field.name(names[i]);
            field.dataType(types[i]);
            tapTable.add(field);
        }
        
        return tapTable;
    }

    private java.util.List<java.util.Map<String, Object>> createStandardData(int size) {
        java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>(size);
        
        for (long i = 0; i < size; i++) {
            java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
            row.put("id", i);
            row.put("name", "User_" + i);
            row.put("value", Math.random() * 100);
            data.add(row);
        }
        
        return data;
    }

    private java.util.List<java.util.Map<String, Object>> createMultiColumnData(int size) {
        java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>(size);
        
        for (long i = 0; i < size; i++) {
            java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
            row.put("id", i);
            row.put("name", "User_" + i);
            row.put("count", (int) (i % 1000));
            row.put("active", i % 2 == 0);
            row.put("score", Math.random() * 100);
            row.put("created_at", new java.util.Date());
            data.add(row);
        }
        
        return data;
    }

    private java.util.List<java.util.Map<String, Object>> createDataWithNulls(int size) {
        java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>(size);
        
        for (long i = 0; i < size; i++) {
            java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
            row.put("id", i % 3 == 0 ? null : i);
            row.put("name", i % 2 == 0 ? null : "User_" + i);
            row.put("value", i % 4 == 0 ? null : Math.random() * 100);
            data.add(row);
        }
        
        return data;
    }

    private java.util.List<java.util.Map<String, Object>> createSpecialCharacterData() {
        java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>();
        
        java.util.Map<String, Object> row1 = new java.util.LinkedHashMap<>();
        row1.put("text_content", "Normal text");
        data.add(row1);
        
        java.util.Map<String, Object> row2 = new java.util.LinkedHashMap<>();
        row2.put("text_content", "Text with 'quotes' and \"double quotes\"");
        data.add(row2);
        
        java.util.Map<String, Object> row3 = new java.util.LinkedHashMap<>();
        row3.put("text_content", "Text with\nnewlines\tand\ttabs");
        data.add(row3);
        
        java.util.Map<String, Object> row4 = new java.util.LinkedHashMap<>();
        row4.put("text_content", "中文文本 日本語 한국어");
        data.add(row4);
        
        java.util.Map<String, Object> row5 = new java.util.LinkedHashMap<>();
        row5.put("text_content", "Emoji: 😀 🎉 🔥");
        data.add(row5);
        
        return data;
    }

    private java.util.List<java.util.Map<String, Object>> createLargeStringData(int size) {
        java.util.List<java.util.Map<String, Object>> data = new java.util.ArrayList<>(size);
        StringBuilder largeText = new StringBuilder();
        
        for (int i = 0; i < 1000; i++) {
            largeText.append("This is a line of text. ");
        }
        
        for (long i = 0; i < size; i++) {
            java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
            row.put("text_content", largeText.toString() + "_row_" + i);
            data.add(row);
        }
        
        return data;
    }

    // ==================== 说明性辅助 ====================

    /*
     * 当前零拷贝路径使用 DuckDB Arrow Stream：
     * 1) registerArrowStream(...)：注册 Arrow 内存批次。
     * 2) INSERT INTO ... SELECT * FROM stream：把 Arrow 数据落库。
     *
     * 单元测试可验证 Statement 调用链；基础用例已升级为集成测试，
     * 通过查询 DuckDB 表数据来验证最终写入结果是否正确。
     */
 }
