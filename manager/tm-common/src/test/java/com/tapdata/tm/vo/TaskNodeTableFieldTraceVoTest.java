package com.tapdata.tm.vo;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for TaskNodeTableFieldTraceVo table and field tracing
 *
 * @author Test
 * @version v1.0 2025/7/3 Create
 */
class TaskNodeTableFieldTraceVoTest {

    private TapTable targetTable;
    private Set<String> sourceFields;
    private String targetDatabaseType;

    @BeforeEach
    void setUp() {
        // Create test TapTable
        targetTable = new TapTable("target_table_id", "target_table");
        targetTable.setAncestorsName("source_table");

        // Create test fields
        LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();

        // Regular fields
        TapField field1 = new TapField("target_field1", "varchar");
        field1.setOriginalFieldName("source_field1");
        fieldMap.put("target_field1", field1);

        TapField field2 = new TapField("target_field2", "int");
        field2.setOriginalFieldName("source_field2");
        fieldMap.put("target_field2", field2);

        // Timestamp field (for testing filtering)
        TapField timestampField = new TapField("timestamp_field", "timestamp");
        timestampField.setOriginalFieldName("source_timestamp");
        fieldMap.put("timestamp_field", timestampField);

        targetTable.setNameFieldMap(fieldMap);

        // Create source fields set
        sourceFields = new HashSet<>();
        sourceFields.add("source_field1");
        sourceFields.add("source_field2");
        sourceFields.add("source_timestamp");

        targetDatabaseType = "MySQL";
    }

    @Nested
    @DisplayName("Normal Case Tests")
    class NormalCaseTests {

        @Test
        @DisplayName("Should create TaskNodeTableFieldTraceVo with matching fields")
        void shouldCreateTaskNodeTableFieldTraceVo_WithMatchingFields() {
            
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, sourceFields, targetDatabaseType);
            
            // Then
            assertNotNull(result);
            assertEquals("source_table", result.getSourceTable());
            assertEquals("target_table", result.getTargetTable());
            assertEquals(3, result.getSourceFields().size());
            assertEquals(3, result.getTargetFields().size());
            assertEquals(3, result.getFieldMap().size());
            
            // Verify field mapping
            assertTrue(result.getSourceFields().contains("source_field1"));
            assertTrue(result.getSourceFields().contains("source_field2"));
            assertTrue(result.getSourceFields().contains("source_timestamp"));

            assertTrue(result.getTargetFields().contains("target_field1"));
            assertTrue(result.getTargetFields().contains("target_field2"));
            assertTrue(result.getTargetFields().contains("timestamp_field"));

            assertEquals("target_field1", result.getFieldMap().get("source_field1"));
            assertEquals("target_field2", result.getFieldMap().get("source_field2"));
            assertEquals("timestamp_field", result.getFieldMap().get("source_timestamp"));
        }

        @Test
        @DisplayName("Should create TaskNodeTableFieldTraceVo with partial matching fields")
        void shouldCreateTaskNodeTableFieldTraceVo_WithPartialMatchingFields() {
            Set<String> partialSourceFields = new HashSet<>();
            partialSourceFields.add("source_field1");
            
            
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, partialSourceFields, targetDatabaseType);
            
            // Then
            assertNotNull(result);
            assertEquals(1, result.getSourceFields().size());
            assertEquals(1, result.getTargetFields().size());
            assertEquals(1, result.getFieldMap().size());
            
            assertTrue(result.getSourceFields().contains("source_field1"));
            assertTrue(result.getTargetFields().contains("target_field1"));
            assertEquals("target_field1", result.getFieldMap().get("source_field1"));
        }
    }

    @Nested
    @DisplayName("Boundary Case Tests")
    class BoundaryCaseTests {

        @Test
        @DisplayName("Should return null when no matching source fields")
        void shouldReturnNull_WhenNoMatchingSourceFields() {
            Set<String> noMatchingFields = new HashSet<>();
            noMatchingFields.add("non_existing_field");
            
            
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, noMatchingFields, targetDatabaseType);
            
            // Then
            assertNull(result);
        }
        
        @Test
        @DisplayName("Should return null when source fields empty")
        void shouldReturnNull_WhenSourceFieldsEmpty() {
            // Given
            Set<String> emptySourceFields = new HashSet<>();

            // When
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, emptySourceFields, targetDatabaseType);

            // Then
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when source fields null")
        void shouldReturnNull_WhenSourceFieldsNull() {
            // When
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, null, targetDatabaseType);

            // Then
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when target table field map null")
        void shouldReturnNull_WhenTargetTableFieldMapNull() {
            // Given
            targetTable.setNameFieldMap(null);

            // When
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, sourceFields, targetDatabaseType);

            // Then
            assertNull(result);
        }

        @Test
        @DisplayName("Should return null when target table field map empty")
        void shouldReturnNull_WhenTargetTableFieldMapEmpty() {
            // Given
            targetTable.setNameFieldMap(new LinkedHashMap<>());

            // When
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, sourceFields, targetDatabaseType);

            // Then
            assertNull(result);
        }
    }

    @Nested
    @DisplayName("Special Database Type Tests")
    class SpecialDatabaseTypeTests {

        @Test
        @DisplayName("Should filter timestamp field for Sybase database")
        void shouldFilterTimestampField_ForSybaseDatabase() {
            // Given
            String sybaseDatabaseType = "Sybase";

            // When
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, sourceFields, sybaseDatabaseType);

            // Then
            assertNotNull(result);
            assertEquals(2, result.getSourceFields().size());
            assertEquals(2, result.getTargetFields().size());
            assertEquals(2, result.getFieldMap().size());

            // Verify timestamp field is filtered out
            assertFalse(result.getSourceFields().contains("source_timestamp"));
            assertFalse(result.getTargetFields().contains("timestamp_field"));
            assertFalse(result.getFieldMap().containsKey("source_timestamp"));

            // Verify other fields still exist
            assertTrue(result.getSourceFields().contains("source_field1"));
            assertTrue(result.getSourceFields().contains("source_field2"));
        }

        @Test
        @DisplayName("Should filter timestamp field for SQL Server database")
        void shouldFilterTimestampField_ForSqlServerDatabase() {
            // Given
            String sqlServerDatabaseType = "SQL Server";

            // When
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, sourceFields, sqlServerDatabaseType);

            // Then
            assertNotNull(result);
            assertEquals(2, result.getSourceFields().size());
            assertEquals(2, result.getTargetFields().size());
            assertEquals(2, result.getFieldMap().size());

            // Verify timestamp field is filtered out
            assertFalse(result.getSourceFields().contains("source_timestamp"));
            assertFalse(result.getTargetFields().contains("timestamp_field"));
            assertFalse(result.getFieldMap().containsKey("source_timestamp"));
        }
        
        @Test
        @DisplayName("Should not filter timestamp field for other databases")
        void shouldNotFilterTimestampField_ForOtherDatabases() {
            // Given
            String mysqlDatabaseType = "MySQL";

            // When
            TaskNodeTableFieldTraceVo result = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, sourceFields, mysqlDatabaseType);

            // Then
            assertNotNull(result);
            assertEquals(3, result.getSourceFields().size());

            // Verify timestamp field is not filtered out
            assertTrue(result.getSourceFields().contains("source_timestamp"));
            assertTrue(result.getTargetFields().contains("timestamp_field"));
            assertTrue(result.getFieldMap().containsKey("source_timestamp"));
        }

        @Test
        @DisplayName("Should ignore case for database type matching")
        void shouldIgnoreCase_ForDatabaseTypeMatching() {
            // Given
            String sybaseLowerCase = "sybase";
            String sqlServerMixedCase = "sql server";

            // When
            TaskNodeTableFieldTraceVo sybaseResult = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, sourceFields, sybaseLowerCase);
            TaskNodeTableFieldTraceVo sqlServerResult = TaskNodeTableFieldTraceVo.ofTargetTable(
                targetTable, sourceFields, sqlServerMixedCase);

            // Then
            assertNotNull(sybaseResult);
            assertNotNull(sqlServerResult);

            // Verify timestamp fields are filtered out
            assertEquals(2, sybaseResult.getSourceFields().size());
            assertEquals(2, sqlServerResult.getSourceFields().size());

            assertFalse(sybaseResult.getSourceFields().contains("source_timestamp"));
            assertFalse(sqlServerResult.getSourceFields().contains("source_timestamp"));
        }
    }
}
