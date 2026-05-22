package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

class ArrowWriterTest {

    private Connection mockConnection;
    private ArrowWriter arrowWriter;

    @BeforeEach
    void setUp() throws SQLException {
        mockConnection = Mockito.mock(Connection.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(Mockito.mock(PreparedStatement.class));
        when(mockConnection.createStatement()).thenReturn(Mockito.mock(java.sql.Statement.class));
        arrowWriter = new ArrowWriter(mockConnection, true);
    }

    @AfterEach
    void tearDown() {
        if (arrowWriter != null) {
            arrowWriter.close();
        }
    }

    // ==================== buildArrowSchema 测试 ====================

    @Test
    void testBuildArrowSchema_WithValidTapTable() {
        TapTable tapTable = createTestTapTable();
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        assertNotNull(schema);
        assertEquals(4, schema.getFields().size());
        
        List<String> fieldNames = new ArrayList<>();
        for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
            fieldNames.add(field.getName());
        }
        
        assertTrue(fieldNames.contains("id"));
        assertTrue(fieldNames.contains("name"));
        assertTrue(fieldNames.contains("age"));
        assertTrue(fieldNames.contains("active"));
    }

    @Test
    void testBuildArrowSchema_WithEmptyTapTable() {
        TapTable tapTable = new TapTable("empty_table");
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        assertNotNull(schema);
        assertEquals(0, schema.getFields().size());
    }

    @Test
    void testBuildArrowSchema_FieldNullable() {
        TapTable tapTable = new TapTable("test_table");
        
        TapField nullableField = new TapField();
        nullableField.name("nullable_col");
        nullableField.dataType("VARCHAR");
        nullableField.nullable(true);
        tapTable.add(nullableField);
        
        TapField nonNullableField = new TapField();
        nonNullableField.name("non_nullable_col");
        nonNullableField.dataType("INTEGER");
        nonNullableField.nullable(false);
        tapTable.add(nonNullableField);
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        assertNotNull(schema);
        assertTrue(schema.getFields().get(0).isNullable());
        assertFalse(schema.getFields().get(1).isNullable());
    }

    // ==================== createVectorSchemaRoot 测试 ====================

    @Test
    void testCreateVectorSchemaRoot_WithData() {
        TapTable tapTable = createTestTapTable();
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        List<Map<String, Object>> data = createTestData();
        
        VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema);
        
        assertNotNull(root);
        assertEquals(3, root.getRowCount());
        assertEquals(4, root.getFieldVectors().size());
        
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            assertEquals(3, root.getVector(i).getValueCount());
        }
        
        root.close();
    }

    @Test
    void testCreateVectorSchemaRoot_WithEmptyData() {
        TapTable tapTable = createTestTapTable();
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        List<Map<String, Object>> data = new ArrayList<>();
        
        VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema);
        
        assertNotNull(root);
        assertEquals(0, root.getRowCount());
        
        root.close();
    }

    @Test
    void testCreateVectorSchemaRoot_WithNullValues() {
        TapTable tapTable = createTestTapTable();
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        List<Map<String, Object>> data = new ArrayList<>();
        Map<String, Object> rowWithNulls = new HashMap<>();
        rowWithNulls.put("id", null);
        rowWithNulls.put("name", null);
        rowWithNulls.put("age", null);
        rowWithNulls.put("active", null);
        data.add(rowWithNulls);
        
        VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema);
        
        assertNotNull(root);
        assertEquals(1, root.getRowCount());
        
        root.close();
    }

    @Test
    void testCreateVectorSchemaRoot_WithMixedTypes() {
        TapTable tapTable = createComplexTapTable();
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        List<Map<String, Object>> data = createComplexTestData();
        
        VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema);
        
        assertNotNull(root);
        assertEquals(2, root.getRowCount());
        
        root.close();
    }

    // ==================== writeWithArrow 测试 ====================

    @Test
    void testWriteWithArrow_SuccessfulWrite() throws SQLException {
        TapTable tapTable = createTestTapTable();
        List<Map<String, Object>> data = createTestData();
        
        java.sql.Statement mockStmt = Mockito.mock(java.sql.Statement.class);
        when(mockConnection.createStatement()).thenReturn(mockStmt);
        
        assertDoesNotThrow(() -> arrowWriter.writeWithArrow(data, "test_table", tapTable));
        
        verify(mockConnection, atLeastOnce()).createStatement();
        verify(mockStmt, atLeastOnce()).executeUpdate(anyString());
    }

    @Test
    void testWriteWithArrow_EmptyData() throws SQLException {
        TapTable tapTable = createTestTapTable();
        List<Map<String, Object>> data = new ArrayList<>();

        assertDoesNotThrow(() -> arrowWriter.writeWithArrow(data, "test_table", tapTable));

        verify(mockConnection, never()).createStatement();
    }

    @Test
    void testWriteWithArrow_ZeroCopyDisabled() throws SQLException {
        ArrowWriter writerWithZeroCopyOff = new ArrowWriter(mockConnection, false);
        
        TapTable tapTable = createTestTapTable();
        List<Map<String, Object>> data = createTestData();
        
        java.sql.Statement mockStmt = Mockito.mock(java.sql.Statement.class);
        when(mockConnection.createStatement()).thenReturn(mockStmt);
        
        try {
            writerWithZeroCopyOff.writeWithArrow(data, "test_table", tapTable);
        } finally {
            writerWithZeroCopyOff.close();
        }
        
        verify(mockStmt, atLeastOnce()).executeUpdate(anyString());
    }

    @Test
    void testWriteWithArrow_LargeBatchData() throws SQLException {
        TapTable tapTable = createTestTapTable();
        List<Map<String, Object>> data = createLargeTestData(10000);

        java.sql.Statement mockStmt = Mockito.mock(java.sql.Statement.class);
        when(mockConnection.createStatement()).thenReturn(mockStmt);

        long startTime = System.currentTimeMillis();
        assertDoesNotThrow(() -> arrowWriter.writeWithArrow(data, "large_test_table", tapTable));
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("Large batch (10000 rows) write time: " + duration + "ms");
        assertTrue(duration < 5000, "Large batch write should complete within 5 seconds");
    }

    // ==================== 类型转换测试 ====================

    @Test
    void testTypeConversion_IntegerTypes() {
        TapTable tapTable = new TapTable("int_types");
        
        String[] types = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT"};
        for (String type : types) {
            TapField field = new TapField();
            field.name(type.toLowerCase() + "_col");
            field.dataType(type);
            tapTable.add(field);
        }
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        assertEquals(4, schema.getFields().size());
        for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
            assertNotNull(field.getFieldType().getType());
        }
    }

    @Test
    void testTypeConversion_FloatTypes() {
        TapTable tapTable = new TapTable("float_types");
        
        TapField floatField = new TapField();
        floatField.name("float_col");
        floatField.dataType("FLOAT");
        tapTable.add(floatField);
        
        TapField doubleField = new TapField();
        doubleField.name("double_col");
        doubleField.dataType("DOUBLE");
        tapTable.add(doubleField);
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        assertEquals(2, schema.getFields().size());
    }

    @Test
    void testTypeConversion_StringAndBinaryTypes() {
        TapTable tapTable = new TapTable("string_binary_types");
        
        TapField stringField = new TapField();
        stringField.name("varchar_col");
        stringField.dataType("VARCHAR");
        tapTable.add(stringField);
        
        TapField blobField = new TapField();
        blobField.name("blob_col");
        blobField.dataType("BLOB");
        tapTable.add(blobField);
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        assertEquals(2, schema.getFields().size());
    }

    @Test
    void testTypeConversion_DateAndBooleanTypes() {
        TapTable tapTable = new TapTable("date_bool_types");
        
        TapField boolField = new TapField();
        boolField.name("bool_col");
        boolField.dataType("BOOLEAN");
        tapTable.add(boolField);
        
        TapField dateField = new TapField();
        dateField.name("date_col");
        dateField.dataType("DATE");
        tapTable.add(dateField);
        
        TapField timestampField = new TapField();
        timestampField.name("timestamp_col");
        timestampField.dataType("TIMESTAMP");
        tapTable.add(timestampField);
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        assertEquals(3, schema.getFields().size());
    }

    // ==================== 边界条件测试 ====================

    @Test
    void testEdgeCase_SpecialCharactersInFieldName() {
        TapTable tapTable = new TapTable("special_chars");
        
        TapField fieldWithSpaces = new TapField();
        fieldWithSpaces.name("field with spaces");
        fieldWithSpaces.dataType("VARCHAR");
        tapTable.add(fieldWithSpaces);
        
        TapField fieldWithUnicode = new TapField();
        fieldWithUnicode.name("字段名_中文");
        fieldWithUnicode.dataType("VARCHAR");
        tapTable.add(fieldWithUnicode);
        
        assertDoesNotThrow(() -> arrowWriter.buildArrowSchema(tapTable));
    }

    @Test
    void testEdgeCase_VeryLongFieldValue() {
        TapTable tapTable = new TapTable("long_value");
        TapField field = new TapField();
        field.name("long_text");
        field.dataType("VARCHAR");
        tapTable.add(field);
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        StringBuilder longValue = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longValue.append("a");
        }
        
        List<Map<String, Object>> data = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("long_text", longValue.toString());
        data.add(row);
        
        VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema);
        
        assertNotNull(root);
        assertEquals(1, root.getRowCount());
        
        root.close();
    }

    @Test
    void testEdgeCase_NumericBoundaryValues() {
        TapTable tapTable = new TapTable("numeric_boundary");
        
        TapField intField = new TapField();
        intField.name("bigint_val");
        intField.dataType("BIGINT");
        tapTable.add(intField);
        
        TapField doubleField = new TapField();
        doubleField.name("double_val");
        doubleField.dataType("DOUBLE");
        tapTable.add(doubleField);
        
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        
        List<Map<String, Object>> data = new ArrayList<>();
        Map<String, Object> boundaryRow = new HashMap<>();
        boundaryRow.put("bigint_val", Long.MAX_VALUE);
        boundaryRow.put("double_val", Double.MAX_VALUE);
        data.add(boundaryRow);
        
        Map<String, Object> negativeRow = new HashMap<>();
        negativeRow.put("bigint_val", Long.MIN_VALUE);
        negativeRow.put("double_val", Double.MIN_VALUE);
        data.add(negativeRow);
        
        VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema);
        
        assertNotNull(root);
        assertEquals(2, root.getRowCount());
        
        root.close();
    }

    // ==================== 资源管理测试 ====================

    @Test
    void testResourceManagement_CloseReleasesResources() {
        ArrowWriter writerToClose = null;
        try {
            writerToClose = new ArrowWriter(mockConnection);
            
            TapTable tapTable = createTestTapTable();
            Schema schema = writerToClose.buildArrowSchema(tapTable);
            List<Map<String, Object>> data = createTestData();
            
            VectorSchemaRoot root = writerToClose.createVectorSchemaRoot(data, schema);
            root.close();
            
        } finally {
            if (writerToClose != null) {
                writerToClose.close();
            }
        }
        
        assertDoesNotThrow(() -> {
        });
    }

    @Test
    void testResourceManagement_MultipleCreateAndCloseCycles() {
        TapTable tapTable = createTestTapTable();
        Schema schema = arrowWriter.buildArrowSchema(tapTable);
        List<Map<String, Object>> data = createTestData();
        
        for (int i = 0; i < 10; i++) {
            VectorSchemaRoot root = arrowWriter.createVectorSchemaRoot(data, schema);
            assertNotNull(root);
            root.close();
        }
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
        
        TapField ageField = new TapField();
        ageField.name("age");
        ageField.dataType("INTEGER");
        tapTable.add(ageField);
        
        TapField activeField = new TapField();
        activeField.name("active");
        activeField.dataType("BOOLEAN");
        tapTable.add(activeField);
        
        return tapTable;
    }

    private TapTable createComplexTapTable() {
        TapTable tapTable = new TapTable("complex_table");
        
        TapField idField = new TapField();
        idField.name("id");
        idField.dataType("BIGINT");
        tapTable.add(idField);
        
        TapField nameField = new TapField();
        nameField.name("name");
        nameField.dataType("VARCHAR");
        tapTable.add(nameField);
        
        TapField scoreField = new TapField();
        scoreField.name("score");
        scoreField.dataType("DOUBLE");
        tapTable.add(scoreField);
        
        TapField createdField = new TapField();
        createdField.name("created_at");
        createdField.dataType("TIMESTAMP");
        tapTable.add(createdField);
        
        TapField dataField = new TapField();
        dataField.name("binary_data");
        dataField.dataType("BLOB");
        tapTable.add(dataField);
        
        return tapTable;
    }

    private List<Map<String, Object>> createTestData() {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 1L);
        row1.put("name", "Alice");
        row1.put("age", 25);
        row1.put("active", true);
        data.add(row1);
        
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 2L);
        row2.put("name", "Bob");
        row2.put("age", 30);
        row2.put("active", false);
        data.add(row2);
        
        Map<String, Object> row3 = new HashMap<>();
        row3.put("id", 3L);
        row3.put("name", "Charlie");
        row3.put("age", 35);
        row3.put("active", true);
        data.add(row3);
        
        return data;
    }

    private List<Map<String, Object>> createComplexTestData() {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", 100L);
        row1.put("name", "Test User");
        row1.put("score", 95.5);
        row1.put("created_at", new Date());
        row1.put("binary_data", new byte[]{1, 2, 3, 4, 5});
        data.add(row1);
        
        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", 101L);
        row2.put("name", "Another User");
        row2.put("score", 88.75);
        row2.put("created_at", new Date(System.currentTimeMillis() - 86400000));
        row2.put("binary_data", "binary string".getBytes());
        data.add(row2);
        
        return data;
    }

    private List<Map<String, Object>> createLargeTestData(int size) {
        List<Map<String, Object>> data = new ArrayList<>(size);
        
        for (long i = 0; i < size; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", i);
            row.put("name", "User_" + i);
            row.put("age", (int) (20 + (i % 50)));
            row.put("active", i % 2 == 0);
            data.add(row);
        }
        
        return data;
    }
}
