package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AffectedKeyCalculatorRefactoredTest {

    private DuckDbOperator mockOperator;
    private Map<String, NodeSchemaInfo> mockSchemaMap;
    private String testQuerySql;
    private List<FromTableConfig> fromTables;

    @BeforeEach
    void setUp() {
        mockOperator = mock(DuckDbOperator.class);
        mockSchemaMap = new HashMap<>();
        testQuerySql = "SELECT * FROM source_1__users WHERE status = 'active'";
        fromTables = new ArrayList<>();
    }

    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {

        @Test
        @DisplayName("Should create instance with valid parameters")
        void testConstructorWithValidParams() {
            AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "wide_table_pk",
                "users",
                "user_id",
                fromTables,
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                testQuerySql
            );

            assertNotNull(calculator);
        }

        @Test
        @DisplayName("Should reject null wideTablePrimaryKey")
        void testConstructorRejectsNullWideTablePk() {
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new AffectedKeyCalculator(
                    null,
                    "users",
                    "user_id",
                    fromTables,
                    Collections.emptyMap(),
                    mockOperator,
                    mockSchemaMap,
                    testQuerySql
                )
            );

            assertTrue(exception.getMessage().contains("wideTablePrimaryKey"));
        }

        @Test
        @DisplayName("Should reject blank wideTablePrimaryKey")
        void testConstructorRejectsBlankWideTablePk() {
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new AffectedKeyCalculator(
                    "   ",
                    "users",
                    "user_id",
                    fromTables,
                    Collections.emptyMap(),
                    mockOperator,
                    mockSchemaMap,
                    testQuerySql
                )
            );

            assertTrue(exception.getMessage().contains("wideTablePrimaryKey"));
        }

        @Test
        @DisplayName("Should reject null operator")
        void testConstructorRejectsNullOperator() {
            NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new AffectedKeyCalculator(
                    "wide_table_pk",
                    "users",
                    "user_id",
                    fromTables,
                    Collections.emptyMap(),
                    null,
                    mockSchemaMap,
                    testQuerySql
                )
            );

            assertTrue(exception.getMessage().contains("operator"));
        }

        @Test
        @DisplayName("Should reject null nodeSchemaMap")
        void testConstructorRejectsNullNodeSchemaMap() {
            NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new AffectedKeyCalculator(
                    "wide_table_pk",
                    "users",
                    "user_id",
                    fromTables,
                    Collections.emptyMap(),
                    mockOperator,
                    null,
                    testQuerySql
                )
            );

            assertTrue(exception.getMessage().contains("nodeSchemaMap"));
        }

        @Test
        @DisplayName("Should reject null resolvedQuerySql")
        void testConstructorRejectsNullResolvedQuerySql() {
            NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> new AffectedKeyCalculator(
                    "wide_table_pk",
                    "users",
                    "user_id",
                    fromTables,
                    Collections.emptyMap(),
                    mockOperator,
                    mockSchemaMap,
                    null
                )
            );

            assertTrue(exception.getMessage().contains("resolvedQuerySql"));
        }

        @Test
        @DisplayName("Should reject blank resolvedQuerySql")
        void testConstructorRejectsBlankResolvedQuerySql() {
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> new AffectedKeyCalculator(
                    "wide_table_pk",
                    "users",
                    "user_id",
                    fromTables,
                    Collections.emptyMap(),
                    mockOperator,
                    mockSchemaMap,
                    "   "
                )
            );

            assertTrue(exception.getMessage().contains("resolvedQuerySql"));
        }

        @Test
        @DisplayName("Should accept empty collections for optional params")
        void testConstructorAcceptsEmptyCollections() {
            AffectedKeyCalculator calculator = new AffectedKeyCalculator(
                "pk",
                "table",
                "id",
                Collections.emptyList(),
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT 1"
            );

            assertNotNull(calculator);
        }
    }

    @Nested
    @DisplayName("findSchemaInfoByTableName Tests")
    class FindSchemaInfoTests {

        private AffectedKeyCalculator calculator;

        @BeforeEach
        void setUp() {
            NodeSchemaInfo mockSchema = Mockito.mock(NodeSchemaInfo.class);
            when(mockSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
            when(mockSchema.getFieldNames()).thenReturn(Arrays.asList("id", "name", "email"));

            // New logic: nodeSchemaMap key is tableName (not nodeId)
            mockSchemaMap.put("users", mockSchema);

            calculator = new AffectedKeyCalculator(
                "pk",
                "users",
                "id",
                fromTables,
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT * FROM target__users"
            );
        }

        @Test
        @DisplayName("Should find schema info by tableName")
        void testFindSchemaByTableNameSuccess() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "findSchemaInfoByTableName", String.class);
            method.setAccessible(true);

            NodeSchemaInfo result = (NodeSchemaInfo) method.invoke(calculator, "users");

            assertNotNull(result);
        }

        @Test
        @DisplayName("Should return null when tableName not found")
        void testFindSchemaByTableNameNotFound() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "findSchemaInfoByTableName", String.class);
            method.setAccessible(true);

            NodeSchemaInfo result = (NodeSchemaInfo) method.invoke(calculator, "nonexistent");

            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when tableName is null")
        void testFindSchemaWithNullTableName() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "findSchemaInfoByTableName", String.class);
            method.setAccessible(true);

            NodeSchemaInfo result = (NodeSchemaInfo) method.invoke(calculator, new Object[]{null});

            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when nodeSchemaMap is empty")
        void testFindSchemaWithEmptyNodeSchemaMap() throws Exception {
            AffectedKeyCalculator emptyCalculator = new AffectedKeyCalculator(
                "pk",
                "table",
                "id",
                Collections.emptyList(),
                Collections.emptyMap(),
                mockOperator,
                Collections.emptyMap(),
                "SELECT 1"
            );

            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "findSchemaInfoByTableName", String.class);
            method.setAccessible(true);

            NodeSchemaInfo result = (NodeSchemaInfo) method.invoke(emptyCalculator, "users");

            assertNull(result);
        }
    }

    @Nested
    @DisplayName("getQuerySqlForTable Tests")
    class GetQuerySqlForTableTests {

        private AffectedKeyCalculator calculator;

        @BeforeEach
        void setUp() {
            calculator = new AffectedKeyCalculator(
                "pk",
                "users",
                "id",
                fromTables,
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT u.id, u.name FROM target__users u WHERE u.status = 'active'"
            );
        }

        @Test
        @DisplayName("Should return resolved query SQL regardless of tableName parameter")
        void testReturnsResolvedQuerySql() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getQuerySqlForTable", String.class);
            method.setAccessible(true);

            String result = (String) method.invoke(calculator, "users");

            assertEquals("SELECT u.id, u.name FROM target__users u WHERE u.status = 'active'", result);
        }

        @Test
        @DisplayName("Should return same SQL for different table names (single query)")
        void testReturnsSameSqlForDifferentTables() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getQuerySqlForTable", String.class);
            method.setAccessible(true);

            String resultUsers = (String) method.invoke(calculator, "users");
            String resultOrders = (String) method.invoke(calculator, "orders");

            assertEquals(resultUsers, resultOrders);
        }

        @Test
        @DisplayName("Should handle long SQL strings correctly")
        void testHandlesLongSqlStrings() throws Exception {
            StringBuilder longSql = new StringBuilder("SELECT ");
            for (int i = 0; i < 200; i++) {
                longSql.append("column_name_").append(i).append(", ");
            }
            longSql.append("id FROM target__table_name");

            AffectedKeyCalculator longSqlCalc = new AffectedKeyCalculator(
                "pk", "t", "id", fromTables,
                Collections.emptyMap(), mockOperator,
                mockSchemaMap, longSql.toString()
            );

            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getQuerySqlForTable", String.class);
            method.setAccessible(true);

            String result = (String) method.invoke(longSqlCalc, "t");

            assertEquals(longSql.toString(), result);
            assertTrue(result.length() > 1000, "SQL should be longer than 1000 chars, actual: " + result.length());
        }
    }

    @Nested
    @DisplayName("getTableFields Tests")
    class GetTableFieldsTests {

        private AffectedKeyCalculator calculator;
        private NodeSchemaInfo mockUserSchema;

        @BeforeEach
        void setUp() {
            mockUserSchema = Mockito.mock(NodeSchemaInfo.class);
            when(mockUserSchema.getFieldNames()).thenReturn(Arrays.asList("user_id", "name", "email", "created_at"));
            when(mockUserSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("user_id"));
            when(mockUserSchema.getFieldMap()).thenReturn(new HashMap<String, TapField>());

            // New logic: nodeSchemaMap key is tableName
            mockSchemaMap.put("users", mockUserSchema);

            calculator = new AffectedKeyCalculator(
                "pk",
                "users",
                "user_id",
                fromTables,
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT * FROM target__users"
            );
        }

        @Test
        @DisplayName("Should return field list from NodeSchemaInfo")
        void testReturnsFieldsFromSchema() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getTableFields", String.class);
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) method.invoke(calculator, "users");

            assertEquals(4, result.size());
            assertTrue(result.contains("user_id"));
            assertTrue(result.contains("name"));
            assertTrue(result.contains("email"));
            assertTrue(result.contains("created_at"));
        }

        @Test
        @DisplayName("Should fallback to empty list when schema not found")
        void testFallbackToEmptyListWhenSchemaNotFound() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getTableFields", String.class);
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) method.invoke(calculator, "nonexistent_table");

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("Should return all field names in order")
        void testPreservesFieldOrder() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getTableFields", String.class);
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            List<String> result = (List<String>) method.invoke(calculator, "users");

            assertEquals("user_id", result.get(0));
            assertEquals("name", result.get(1));
            assertEquals("email", result.get(2));
            assertEquals("created_at", result.get(3));
        }
    }

    @Nested
    @DisplayName("getSourceTablePrimaryKey Tests")
    class GetSourceTablePrimaryKeyTests {

        private AffectedKeyCalculator calculator;
        private NodeSchemaInfo mockUserSchema;

        @BeforeEach
        void setUp() {
            mockUserSchema = Mockito.mock(NodeSchemaInfo.class);
            when(mockUserSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("user_id"));
            when(mockUserSchema.getFieldNames()).thenReturn(Arrays.asList("user_id", "name"));
            when(mockUserSchema.getFieldMap()).thenReturn(new HashMap<String, TapField>());

            // New logic: nodeSchemaMap key is tableName
            mockSchemaMap.put("users", mockUserSchema);

            calculator = new AffectedKeyCalculator(
                "pk",
                "users",
                "user_id",
                fromTables,
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT * FROM target__users"
            );
        }

        @Test
        @DisplayName("Should return primary key from NodeSchemaInfo")
        void testReturnsPrimaryKeyFromSchema() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getSourceTablePrimaryKey", String.class);
            method.setAccessible(true);

            String result = (String) method.invoke(calculator, "users");

            assertEquals("user_id", result);
        }

        @Test
        @DisplayName("Should detect common PK name 'id' as fallback")
        void testDetectsCommonIdAsFallback() throws Exception {
            when(mockUserSchema.getPrimaryKeys()).thenReturn(Collections.emptyList());

            Map<String, TapField> fieldMap = new HashMap<>();
            fieldMap.put("id", Mockito.mock(TapField.class));
            when(mockUserSchema.getFieldMap()).thenReturn(fieldMap);
            when(mockUserSchema.getFieldNames()).thenReturn(Arrays.asList("id", "name"));

            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getSourceTablePrimaryKey", String.class);
            method.setAccessible(true);

            String result = (String) method.invoke(calculator, "users");

            assertEquals("id", result);
        }

        @Test
        @DisplayName("Should detect common PK name '_id' as fallback")
        void testDetectsUnderscoreIdAsFallback() throws Exception {
            when(mockUserSchema.getPrimaryKeys()).thenReturn(Collections.emptyList());

            Map<String, TapField> fieldMap = new HashMap<>();
            fieldMap.put("_id", Mockito.mock(TapField.class));
            when(mockUserSchema.getFieldMap()).thenReturn(fieldMap);
            when(mockUserSchema.getFieldNames()).thenReturn(Arrays.asList("_id", "name"));

            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getSourceTablePrimaryKey", String.class);
            method.setAccessible(true);

            String result = (String) method.invoke(calculator, "users");

            assertEquals("_id", result);
        }

        @Test
        @DisplayName("Should throw exception when schema not found")
        void testThrowsWhenSchemaNotFound() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getSourceTablePrimaryKey", String.class);
            method.setAccessible(true);

            try {
                method.invoke(calculator, "nonexistent_table");
                fail("Expected IllegalStateException to be thrown");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue(e.getCause() instanceof IllegalStateException);
                IllegalStateException exception = (IllegalStateException) e.getCause();
                assertTrue(exception.getMessage().contains("Failed to find schema info"));
                assertTrue(exception.getMessage().contains("nonexistent_table"));
            }
        }

        @Test
        @DisplayName("Should throw exception when no PK and no common names found")
        void testThrowsWhenNoPkAndNoCommonNames() throws Exception {
            when(mockUserSchema.getPrimaryKeys()).thenReturn(Collections.emptyList());
            when(mockUserSchema.getFieldMap()).thenReturn(new HashMap<String, TapField>());
            when(mockUserSchema.getFieldNames()).thenReturn(Arrays.asList("name", "email"));

            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "getSourceTablePrimaryKey", String.class);
            method.setAccessible(true);

            try {
                method.invoke(calculator, "users");
                fail("Expected IllegalStateException to be thrown");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue(e.getCause() instanceof IllegalStateException);
                IllegalStateException exception = (IllegalStateException) e.getCause();
                assertTrue(exception.getMessage().contains("No primary key found"));
                assertTrue(exception.getMessage().contains("name, email"));
            }
        }
    }

    @Nested
    @DisplayName("Integration Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Should work end-to-end with complete schema setup")
        void testEndToEndWithCompleteSchema() throws Exception {
            NodeSchemaInfo userSchema = Mockito.mock(NodeSchemaInfo.class);
            when(userSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("user_id"));
            when(userSchema.getFieldNames()).thenReturn(Arrays.asList("user_id", "name", "email"));
            when(userSchema.getFieldMap()).thenReturn(new HashMap<String, TapField>());

            NodeSchemaInfo orderSchema = Mockito.mock(NodeSchemaInfo.class);
            when(orderSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("order_id"));
            when(orderSchema.getFieldNames()).thenReturn(Arrays.asList("order_id", "user_id", "total"));
            when(orderSchema.getFieldMap()).thenReturn(new HashMap<String, TapField>());

            // New logic: nodeSchemaMap key is tableName (as used in SQL)
            mockSchemaMap.put("u", userSchema);
            mockSchemaMap.put("o", orderSchema);

            AffectedKeyCalculator calc = new AffectedKeyCalculator(
                "id",
                "u",
                "user_id",
                fromTables,
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT u.user_id, o.order_id FROM target__users u JOIN target__orders o ON u.user_id = o.user_id"
            );

            java.lang.reflect.Method getQuerySql = AffectedKeyCalculator.class.getDeclaredMethod(
                "getQuerySqlForTable", String.class);
            getQuerySql.setAccessible(true);

            java.lang.reflect.Method getFields = AffectedKeyCalculator.class.getDeclaredMethod(
                "getTableFields", String.class);
            getFields.setAccessible(true);

            java.lang.reflect.Method getPk = AffectedKeyCalculator.class.getDeclaredMethod(
                "getSourceTablePrimaryKey", String.class);
            getPk.setAccessible(true);

            String querySql = (String) getQuerySql.invoke(calc, "u");
            assertNotNull(querySql);
            assertTrue(querySql.contains("target__users"));
            assertTrue(querySql.contains("target__orders"));

            @SuppressWarnings("unchecked")
            List<String> userFields = (List<String>) getFields.invoke(calc, "u");
            assertEquals(3, userFields.size());
            assertTrue(userFields.contains("user_id"));

            @SuppressWarnings("unchecked")
            List<String> orderFields = (List<String>) getFields.invoke(calc, "o");
            assertEquals(3, orderFields.size());
            assertTrue(orderFields.contains("order_id"));

            String userPk = (String) getPk.invoke(calc, "u");
            assertEquals("user_id", userPk);

            String orderPk = (String) getPk.invoke(calc, "o");
            assertEquals("order_id", orderPk);
        }

        @Test
        @DisplayName("Should handle multi-table scenario with fallbacks correctly")
        void testMultiTableScenarioWithFallbacks() throws Exception {
            NodeSchemaInfo schemaWithPk = Mockito.mock(NodeSchemaInfo.class);
            when(schemaWithPk.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
            when(schemaWithPk.getFieldNames()).thenReturn(Arrays.asList("id", "value"));
            Map<String, TapField> fieldMap1 = new HashMap<>();
            fieldMap1.put("id", Mockito.mock(TapField.class));
            when(schemaWithPk.getFieldMap()).thenReturn(fieldMap1);

            NodeSchemaInfo schemaWithoutPk = Mockito.mock(NodeSchemaInfo.class);
            when(schemaWithoutPk.getPrimaryKeys()).thenReturn(Collections.emptyList());
            when(schemaWithoutPk.getFieldNames()).thenReturn(Arrays.asList("_id", "data"));
            Map<String, TapField> fieldMap2 = new HashMap<>();
            fieldMap2.put("_id", Mockito.mock(TapField.class));
            when(schemaWithoutPk.getFieldMap()).thenReturn(fieldMap2);

            // New logic: nodeSchemaMap key is tableName (as used in SQL)
            mockSchemaMap.put("t1", schemaWithPk);
            mockSchemaMap.put("t2", schemaWithoutPk);

            AffectedKeyCalculator calc = new AffectedKeyCalculator(
                "pk",
                "t1",
                "id",
                fromTables,
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT * FROM target__t1 t1 LEFT JOIN target__t2 t2 ON t1.id = t2._id"
            );

            java.lang.reflect.Method getPk = AffectedKeyCalculator.class.getDeclaredMethod(
                "getSourceTablePrimaryKey", String.class);
            getPk.setAccessible(true);

            String pk1 = (String) getPk.invoke(calc, "t1");
            assertEquals("id", pk1);

            String pk2 = (String) getPk.invoke(calc, "t2");
            assertEquals("_id", pk2);
        }
    }

    @Nested
    @DisplayName("Main Table Optimization Tests")
    class MainTableOptimizationTests {

        private AffectedKeyCalculator calculator;

        @BeforeEach
        void setUp() {
            NodeSchemaInfo userSchema = mock(NodeSchemaInfo.class);
            when(userSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
            when(userSchema.getFieldNames()).thenReturn(Arrays.asList("id", "name", "email"));
            when(userSchema.getFieldMap()).thenReturn(new HashMap<>());
            
            mockSchemaMap.put("node_users", userSchema);
            fromTables.add(new FromTableConfig("node_users", "users"));
            
            calculator = new AffectedKeyCalculator(
                "pk",
                "users",
                "id",
                fromTables,
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT * FROM target__users"
            );
        }

        @Test
        @DisplayName("Test calculateAffectedBeforeKeys for main table - optimized path")
        void testCalculateAffectedBeforeKeys_MainTable_OptimizedPath() throws SQLException {
            List<SmartMerger.MergedRecord> mergedRecords = new ArrayList<>();
            
            // Add UPDATE event (before key = 1)
            SmartMerger.MergedRecord updateRecord = createMergedRecord("users", 1, 
                createRow("id", 1, "name", "Alice_old"), 
                createRow("id", 1, "name", "Alice_new"));
            mergedRecords.add(updateRecord);
            
            // Add DELETE event (before key = 2)
            SmartMerger.MergedRecord deleteRecord = createMergedRecord("users", 2, 
                createRow("id", 2, "name", "Bob"), 
                null);
            mergedRecords.add(deleteRecord);

            Set<Object> result = calculator.calculateAffectedBeforeKeys(mergedRecords, "users");

            assertTrue(result.contains(1));
            assertTrue(result.contains(2));
            assertEquals(2, result.size());
            verify(mockOperator, never()).executeQuery(anyString());
        }

        @Test
        @DisplayName("Test calculateAffectedAfterKeys for main table - returns AffectedKeysResult")
        void testCalculateAffectedAfterKeys_MainTable_OptimizedPath() throws SQLException {
            List<SmartMerger.MergedRecord> mergedRecords = new ArrayList<>();
            
            // Add INSERT event (after key = 3)
            SmartMerger.MergedRecord insertRecord = createMergedRecord("users", 3, 
                null, 
                createRow("id", 3, "name", "Charlie"));
            mergedRecords.add(insertRecord);
            
            // Add UPDATE event (after key = 1)
            SmartMerger.MergedRecord updateRecord = createMergedRecord("users", 1, 
                createRow("id", 1, "name", "Alice_old"), 
                createRow("id", 1, "name", "Alice_new"));
            mergedRecords.add(updateRecord);

            AffectedKeyCalculator.AffectedKeysResult result = calculator.calculateAffectedAfterKeys(mergedRecords, "users");

            assertNotNull(result);
            assertTrue(result.getWideTablePks().contains(1));
            assertTrue(result.getWideTablePks().contains(3));
            assertEquals(2, result.getWideTablePks().size());
            assertNotNull(result.getAfterRows());
        }
        
        @Test
        @DisplayName("Test AffectedKeysResult contains afterRows")
        void testAffectedKeysResultContainsAfterRows() throws SQLException {
            List<SmartMerger.MergedRecord> mergedRecords = new ArrayList<>();
            
            Map<String, Object> afterRow = createRow("id", 1, "name", "Alice");
            SmartMerger.MergedRecord record = createMergedRecord("users", 1, null, afterRow);
            mergedRecords.add(record);

            AffectedKeyCalculator.AffectedKeysResult result = calculator.calculateAffectedAfterKeys(mergedRecords, "users");

            assertNotNull(result);
            assertNotNull(result.getAfterRows());
            assertEquals(1, result.getAfterRows().size());
            assertEquals("Alice", result.getAfterRows().get(0).get("name"));
        }
        
        @Test
        @DisplayName("Test AffectedKeysResult with empty input returns empty result")
        void testCalculateAffectedAfterKeys_EmptyInput() throws SQLException {
            List<SmartMerger.MergedRecord> mergedRecords = new ArrayList<>();

            AffectedKeyCalculator.AffectedKeysResult result = calculator.calculateAffectedAfterKeys(mergedRecords, "users");

            assertNotNull(result);
            assertTrue(result.isEmpty());
            assertTrue(result.getWideTablePks().isEmpty());
            assertTrue(result.getAfterRows().isEmpty());
            assertTrue(result.getWideTableQueryResults().isEmpty());
        }
        
        @Test
        @DisplayName("Test AffectedKeysResult with null input returns empty result")
        void testCalculateAffectedAfterKeys_NullInput() throws SQLException {
            AffectedKeyCalculator.AffectedKeysResult result = calculator.calculateAffectedAfterKeys(null, "users");

            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        /**
         * Helper method to create a MergedRecord with before/after rows
         */
        private SmartMerger.MergedRecord createMergedRecord(String tableName, Object pk, 
                                                             Map<String, Object> beforeRow, 
                                                             Map<String, Object> afterRow) {
            SmartMerger.MergedRecord record = new SmartMerger.MergedRecord();
            record.setTableName(tableName);
            
            if (beforeRow != null) {
                record.getBeforeRows().add(beforeRow);
                record.getMainTableBeforePks().add(pk);
            }
            
            if (afterRow != null) {
                record.setAfterRow(pk.toString(), afterRow);
                record.getMainTableAfterPks().add(pk);
            }
            
            return record;
        }
        
        /**
         * Helper method to create a row map
         */
        private Map<String, Object> createRow(Object... kvPairs) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < kvPairs.length; i += 2) {
                row.put((String) kvPairs[i], kvPairs[i + 1]);
            }
            return row;
        }
    }
    
    @Nested
    @DisplayName("AffectedKeysResult Tests")
    class AffectedKeysResultTests {
        
        @Test
        @DisplayName("Test AffectedKeysResult constructor and getters")
        void testConstructorAndGetters() {
            Set<Object> pks = new LinkedHashSet<>(Arrays.asList(1, 2, 3));
            List<Map<String, Object>> queryResults = Arrays.asList(
                createRow("pk", 1, "name", "Alice"),
                createRow("pk", 2, "name", "Bob")
            );
            List<Map<String, Object>> afterRows = Arrays.asList(
                createRow("id", 1, "value", "a"),
                createRow("id", 2, "value", "b")
            );
            
            AffectedKeyCalculator.AffectedKeysResult result = 
                new AffectedKeyCalculator.AffectedKeysResult(pks, queryResults, afterRows);
            
            assertEquals(3, result.getWideTablePks().size());
            assertTrue(result.getWideTablePks().contains(1));
            
            assertEquals(2, result.getWideTableQueryResults().size());
            assertEquals("Alice", result.getWideTableQueryResults().get(0).get("name"));
            
            assertEquals(2, result.getAfterRows().size());
            assertEquals("a", result.getAfterRows().get(0).get("value"));
        }
        
        @Test
        @DisplayName("Test AffectedKeysResult isEmpty method")
        void testIsEmpty() {
            // Empty result
            AffectedKeyCalculator.AffectedKeysResult emptyResult = 
                new AffectedKeyCalculator.AffectedKeysResult(
                    Collections.emptySet(), 
                    Collections.emptyList(), 
                    Collections.emptyList());
            assertTrue(emptyResult.isEmpty());
            
            // Non-empty PKs
            AffectedKeyCalculator.AffectedKeysResult nonEmpty1 = 
                new AffectedKeyCalculator.AffectedKeysResult(
                    new LinkedHashSet<>(Arrays.asList(1)), 
                    Collections.emptyList(), 
                    Collections.emptyList());
            assertFalse(nonEmpty1.isEmpty());
            
            // Non-empty queryResults
            AffectedKeyCalculator.AffectedKeysResult nonEmpty2 = 
                new AffectedKeyCalculator.AffectedKeysResult(
                    Collections.emptySet(), 
                    Arrays.asList(createRow("pk", 1)), 
                    Collections.emptyList());
            assertFalse(nonEmpty2.isEmpty());
            
            // Non-empty afterRows
            AffectedKeyCalculator.AffectedKeysResult nonEmpty3 = 
                new AffectedKeyCalculator.AffectedKeysResult(
                    Collections.emptySet(), 
                    Collections.emptyList(), 
                    Arrays.asList(createRow("id", 1)));
            assertFalse(nonEmpty3.isEmpty());
        }
        
        @Test
        @DisplayName("Test AffectedKeysResult with null values")
        void testConstructorWithNulls() {
            AffectedKeyCalculator.AffectedKeysResult result = 
                new AffectedKeyCalculator.AffectedKeysResult(null, null, null);
            
            assertNull(result.getWideTablePks());
            assertNull(result.getWideTableQueryResults());
            assertNull(result.getAfterRows());
            assertTrue(result.isEmpty());  // null fields should be treated as empty
        }
        
        private Map<String, Object> createRow(Object... kvPairs) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < kvPairs.length; i += 2) {
                row.put((String) kvPairs[i], kvPairs[i + 1]);
            }
            return row;
        }
    }
}
