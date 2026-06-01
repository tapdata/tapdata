package io.tapdata.flow.engine.V2.node.duckdb;

// import com.tapdata.entity.TapdataEvent;
// import io.tapdata.entity.event.dml.TapInsertRecordEvent;
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
    @DisplayName("findSchemaInfoByTableNameInSql Tests")
    class FindSchemaInfoTests {

        private AffectedKeyCalculator calculator;

        @BeforeEach
        void setUp() {
            NodeSchemaInfo mockSchema = Mockito.mock(NodeSchemaInfo.class);
            when(mockSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
            when(mockSchema.getFieldNames()).thenReturn(Arrays.asList("id", "name", "email"));

            mockSchemaMap.put("node_mysql_1", mockSchema);

            fromTables.add(new FromTableConfig("node_mysql_1", "users"));

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
        @DisplayName("Should find schema info by matching tableNameInSql")
        void testFindSchemaByTableNameInSqlSuccess() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "findSchemaInfoByTableNameInSql", String.class);
            method.setAccessible(true);

            NodeSchemaInfo result = (NodeSchemaInfo) method.invoke(calculator, "users");

            assertNotNull(result);
        }

        @Test
        @DisplayName("Should return null when tableNameInSql not found")
        void testFindSchemaByTableNameInSqlNotFound() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "findSchemaInfoByTableNameInSql", String.class);
            method.setAccessible(true);

            NodeSchemaInfo result = (NodeSchemaInfo) method.invoke(calculator, "nonexistent");

            assertNull(result);
        }

        @Test
        @DisplayName("Should be case-insensitive for tableNameInSql matching")
        void testFindSchemaCaseInsensitive() throws Exception {
            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "findSchemaInfoByTableNameInSql", String.class);
            method.setAccessible(true);

            NodeSchemaInfo resultUpper = (NodeSchemaInfo) method.invoke(calculator, "USERS");
            NodeSchemaInfo resultLower = (NodeSchemaInfo) method.invoke(calculator, "users");

            assertNotNull(resultUpper);
            assertNotNull(resultLower);
            assertEquals(resultUpper, resultLower);
        }

        @Test
        @DisplayName("Should return null when fromTables is empty")
        void testFindSchemaWithEmptyFromTables() throws Exception {
            AffectedKeyCalculator emptyCalculator = new AffectedKeyCalculator(
                "pk",
                "table",
                "id",
                Collections.emptyList(),
                Collections.emptyMap(),
                mockOperator,
                mockSchemaMap,
                "SELECT 1"
            );

            java.lang.reflect.Method method = AffectedKeyCalculator.class.getDeclaredMethod(
                "findSchemaInfoByTableNameInSql", String.class);
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

            mockSchemaMap.put("node_mysql_1", mockUserSchema);
            fromTables.add(new FromTableConfig("node_mysql_1", "users"));

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

            mockSchemaMap.put("node_mysql_1", mockUserSchema);
            fromTables.add(new FromTableConfig("node_mysql_1", "users"));

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

            mockSchemaMap.put("node_users", userSchema);
            mockSchemaMap.put("node_orders", orderSchema);

            fromTables.add(new FromTableConfig("node_users", "u"));
            fromTables.add(new FromTableConfig("node_orders", "o"));

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

            mockSchemaMap.put("node_table1", schemaWithPk);
            mockSchemaMap.put("node_table2", schemaWithoutPk);

            fromTables.add(new FromTableConfig("node_table1", "t1"));
            fromTables.add(new FromTableConfig("node_table2", "t2"));

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

    // @Nested
    // @DisplayName("Main Table Optimization Tests")
    // class MainTableOptimizationTests {
    //
    //     private AffectedKeyCalculator calculator;
    //
    //     @BeforeEach
    //     void setUp() {
    //         NodeSchemaInfo userSchema = mock(NodeSchemaInfo.class);
    //         when(userSchema.getPrimaryKeys()).thenReturn(Collections.singletonList("id"));
    //         when(userSchema.getFieldNames()).thenReturn(Arrays.asList("id", "name", "email"));
    //         when(userSchema.getFieldMap()).thenReturn(new HashMap<>());
    //         
    //         mockSchemaMap.put("node_users", userSchema);
    //         fromTables.add(new FromTableConfig("node_users", "users"));
    //         
    //         calculator = new AffectedKeyCalculator(
    //             "pk",
    //             "users",
    //             "id",
    //             fromTables,
    //             Collections.emptyMap(),
    //             mockOperator,
    //             mockSchemaMap,
    //             "SELECT * FROM target__users"
    //         );
    //     }
    //
    //     @Test
    //     @DisplayName("testCalculateAffectedBeforeKeys_MainTable_OptimizedPath")
    //     void testCalculateAffectedBeforeKeys_MainTable_OptimizedPath() throws SQLException {
    //         List<TapdataEvent> events = new ArrayList<>();
    //         events.add(createTapdataUpdateEvent("users", "id", 1, "name", "Alice"));
    //         events.add(createTapdataDeleteEvent("users", "id", 2));
    //
    //         Set<Object> result = calculator.calculateAffectedBeforeKeys(events, "users");
    //
    //         assertTrue(result.contains(1L));
    //         assertTrue(result.contains(2L));
    //         assertEquals(2, result.size());
    //         verify(mockOperator, never()).executeQuery(anyString());
    //     }
    //
    //     @Test
    //     @DisplayName("testCalculateAffectedAfterKeys_MainTable_OptimizedPath")
    //     void testCalculateAffectedAfterKeys_MainTable_OptimizedPath() throws SQLException {
    //         List<TapdataEvent> events = new ArrayList<>();
    //         events.add(createTapdataInsertEvent("users", "id", 3, "name", "Charlie"));
    //         events.add(createTapdataUpdateEvent("users", "id", 1, "name", "Alice"));
    //
    //         Set<Object> result = calculator.calculateAffectedAfterKeys(events);
    //
    //         assertTrue(result.contains(1L));
    //         assertTrue(result.contains(3L));
    //         assertEquals(2, result.size());
    //         verify(mockOperator, never()).executeQuery(anyString());
    //     }
    //
    //     private TapdataEvent createTapdataInsertEvent(String tableName, Object key, Object value, String name, Object nameValue) {
    //         TapdataEvent tapdataEvent = new TapdataEvent();
    //         TapInsertRecordEvent insertEvent = new TapInsertRecordEvent();
    //         insertEvent.setTableId(tableName);
    //
    //         Map<String, Object> after = new HashMap<>();
    //         after.put((String) key, value);
    //         after.put(name, nameValue);
    //         insertEvent.setAfter(after);
    //
    //         tapdataEvent.setTapEvent(insertEvent);
    //         return tapdataEvent;
    //     }
    //
    //     private TapdataEvent createTapdataUpdateEvent(String tableName, Object key, Object value, String name, Object nameValue) {
    //         TapdataEvent tapdataEvent = new TapdataEvent();
    //         io.tapdata.entity.event.dml.TapUpdateRecordEvent updateEvent = new io.tapdata.entity.event.dml.TapUpdateRecordEvent();
    //         updateEvent.setTableId(tableName);
    //
    //         Map<String, Object> before = new HashMap<>();
    //         Map<String, Object> after = new HashMap<>();
    //         before.put((String) key, value);
    //         after.put((String) key, value);
    //         after.put(name, nameValue);
    //         updateEvent.setBefore(before);
    //         updateEvent.setAfter(after);
    //
    //         tapdataEvent.setTapEvent(updateEvent);
    //         return tapdataEvent;
    //     }
    //
    //     private TapdataEvent createTapdataDeleteEvent(String tableName, Object key, Object value) {
    //         TapdataEvent tapdataEvent = new TapdataEvent();
    //         io.tapdata.entity.event.dml.TapDeleteRecordEvent deleteEvent = new io.tapdata.entity.event.dml.TapDeleteRecordEvent();
    //         deleteEvent.setTableId(tableName);
    //
    //         Map<String, Object> before = new HashMap<>();
    //         before.put((String) key, value);
    //         deleteEvent.setBefore(before);
    //
    //         tapdataEvent.setTapEvent(deleteEvent);
    //         return tapdataEvent;
    //     }
    // }
}
