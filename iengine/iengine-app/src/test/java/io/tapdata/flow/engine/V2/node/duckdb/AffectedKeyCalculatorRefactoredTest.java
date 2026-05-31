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
}
