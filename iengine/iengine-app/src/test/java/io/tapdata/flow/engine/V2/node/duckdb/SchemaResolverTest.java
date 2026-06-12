package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SchemaResolverTest {

    @Mock
    private DuckDbOperator mockDuckDbOperator;

    private SchemaResolver schemaResolver;

    @BeforeEach
    void setUp() {
        schemaResolver = new SchemaResolver(mockDuckDbOperator);
    }

    // ==================== resolveFields ====================

    @Test
    void testResolveFields_fromAfterData() {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        after.put("name", "John");
        after.put("email", "john@example.com");
        event.put("after", after);

        List<String> result = schemaResolver.resolveFields("users", event);

        assertEquals(3, result.size());
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("email"));
    }

    @Test
    void testResolveFields_fromBeforeData() {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put("order_id", "ORD001");
        before.put("user_id", 123);
        event.put("before", before);

        List<String> result = schemaResolver.resolveFields("orders", event);

        assertEquals(2, result.size());
        assertTrue(result.contains("order_id"));
        assertTrue(result.contains("user_id"));
    }

    @Test
    void testResolveFields_afterPreferredOverBefore() {
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        before.put("old_field", "value");
        event.put("before", before);

        Map<String, Object> after = new HashMap<>();
        after.put("new_field", "value");
        event.put("after", after);

        List<String> result = schemaResolver.resolveFields("users", event);

        assertEquals(1, result.size());
        assertTrue(result.contains("new_field"));
    }

    @Test
    void testResolveFields_fallbackToMetadata() throws SQLException {
        Map<String, Object> event = new HashMap<>();
        // No before/after data

        List<Map<String, Object>> metadataResult = List.of(
            Map.of("column_name", "id"),
            Map.of("column_name", "name"),
            Map.of("column_name", "created_at")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(metadataResult);

        List<String> result = schemaResolver.resolveFields("users", event);

        assertEquals(3, result.size());
        assertEquals("id", result.get(0));
        assertEquals("name", result.get(1));
        assertEquals("created_at", result.get(2));
    }

    @Test
    void testResolveFields_emptyAfterAndBefore() throws SQLException {
        Map<String, Object> event = new HashMap<>();
        event.put("after", new HashMap<>());
        event.put("before", new HashMap<>());

        List<Map<String, Object>> metadataResult = List.of(
            Map.of("column_name", "id")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(metadataResult);

        List<String> result = schemaResolver.resolveFields("users", event);

        assertEquals(1, result.size());
        assertTrue(result.contains("id"));
    }

    // ==================== resolveFieldsFromMetadata ====================

    @Test
    void testResolveFieldsFromMetadata_success() throws SQLException {
        List<Map<String, Object>> metadataResult = List.of(
            Map.of("column_name", "id"),
            Map.of("column_name", "name"),
            Map.of("column_name", "email")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(metadataResult);

        List<String> result = schemaResolver.resolveFieldsFromMetadata("users");

        assertEquals(3, result.size());
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("email"));
    }

    @Test
    void testResolveFieldsFromMetadata_emptyTable() throws SQLException {
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(List.of());

        List<String> result = schemaResolver.resolveFieldsFromMetadata("empty_table");

        assertTrue(result.isEmpty());
    }

    @Test
    void testResolveFieldsFromMetadata_sqlException() throws SQLException {
        when(mockDuckDbOperator.executeQuery(anyString())).thenThrow(new SQLException("Table not found"));

        List<String> result = schemaResolver.resolveFieldsFromMetadata("nonexistent_table");

        assertTrue(result.isEmpty());
    }

    @Test
    void testResolveFieldsFromMetadata_nullColumnName() throws SQLException {
        List<Map<String, Object>> metadataResult = List.of(
            Map.of("column_name", "id"),
            Map.of("other_field", "value"),
            Map.of("column_name", "name")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(metadataResult);

        List<String> result = schemaResolver.resolveFieldsFromMetadata("users");

        assertEquals(2, result.size());
        assertTrue(result.contains("id"));
        assertTrue(result.contains("name"));
    }
}
