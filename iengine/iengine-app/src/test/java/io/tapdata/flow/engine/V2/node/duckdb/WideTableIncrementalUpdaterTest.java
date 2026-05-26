package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.*;

import static io.tapdata.flow.engine.V2.node.duckdb.WideTableCdcEvent.OpType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WideTableIncrementalUpdaterTest {

    @Mock
    private DuckDbOperator mockDuckDbOperator;

    private WideTableIncrementalUpdater updater;

    @BeforeEach
    void setUp() {
        List<String> fields = Arrays.asList("id", "name", "email");
        updater = new WideTableIncrementalUpdater("id", 
                "SELECT id, name, email FROM users", 
                fields, mockDuckDbOperator);
    }

    // ==================== updateWideTable ====================

    @Test
    void testUpdateWideTable_deleteAndInsert() throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Arrays.asList(123, 789));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Arrays.asList(456, 789));

        List<Map<String, Object>> queryResult = Arrays.asList(
                createRow(456, "John", "john@example.com"),
                createRow(789, "Jane Updated", "jane@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<WideTableCdcEvent> result = updater.updateWideTable(affectedBeforeKeys, affectedAfterKeys);

        // 验证 DELETE 事件 (123 在 before 中但不在 after 中)
        List<WideTableCdcEvent> deleteEvents = filterByOp(result, DELETE);
        assertEquals(1, deleteEvents.size());
        assertEquals(123, deleteEvents.get(0).getPrimaryKey());

        // 验证 INSERT 事件 (456 不在 before 中)
        List<WideTableCdcEvent> insertEvents = filterByOp(result, INSERT);
        assertEquals(1, insertEvents.size());
        assertEquals(456, insertEvents.get(0).getPrimaryKey());

        // 验证 UPDATE 事件 (789 在 before 和 after 中)
        List<WideTableCdcEvent> updateEvents = filterByOp(result, UPDATE);
        assertEquals(1, updateEvents.size());
        assertEquals(789, updateEvents.get(0).getPrimaryKey());
    }

    @Test
    void testUpdateWideTable_primaryKeyUpdate() throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(456));

        List<Map<String, Object>> queryResult = Collections.singletonList(
                createRow(456, "John", "john@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<WideTableCdcEvent> result = updater.updateWideTable(affectedBeforeKeys, affectedAfterKeys);

        assertEquals(2, result.size());

        // 验证 DELETE 事件
        assertEquals(DELETE, result.get(0).getOpType());
        assertEquals(123, result.get(0).getPrimaryKey());

        // 验证 INSERT 事件
        assertEquals(INSERT, result.get(1).getOpType());
        assertEquals(456, result.get(1).getPrimaryKey());
    }

    @Test
    void testUpdateWideTable_onlyDelete() throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Arrays.asList(123, 456));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();

        List<WideTableCdcEvent> result = updater.updateWideTable(affectedBeforeKeys, affectedAfterKeys);

        assertEquals(2, result.size());
        assertEquals(DELETE, result.get(0).getOpType());
        assertEquals(123, result.get(0).getPrimaryKey());
        assertEquals(DELETE, result.get(1).getOpType());
        assertEquals(456, result.get(1).getPrimaryKey());
    }

    @Test
    void testUpdateWideTable_onlyInsert() throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Arrays.asList(123, 456));

        List<Map<String, Object>> queryResult = Arrays.asList(
                createRow(123, "John", "john@example.com"),
                createRow(456, "Jane", "jane@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<WideTableCdcEvent> result = updater.updateWideTable(affectedBeforeKeys, affectedAfterKeys);

        assertEquals(2, result.size());
        assertEquals(INSERT, result.get(0).getOpType());
        assertEquals(INSERT, result.get(1).getOpType());
    }

    @Test
    void testUpdateWideTable_emptyBoth() throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();

        List<WideTableCdcEvent> result = updater.updateWideTable(affectedBeforeKeys, affectedAfterKeys);

        assertTrue(result.isEmpty());
    }

    @Test
    void testUpdateWideTable_noChange() throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(123));

        List<Map<String, Object>> queryResult = Collections.singletonList(
                createRow(123, "John", "john@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<WideTableCdcEvent> result = updater.updateWideTable(affectedBeforeKeys, affectedAfterKeys);

        assertEquals(1, result.size());
        assertEquals(UPDATE, result.get(0).getOpType());
        assertEquals(123, result.get(0).getPrimaryKey());
    }

    @Test
    void testUpdateWideTable_WithCteIntegration() throws SQLException {
        List<String> fields = Arrays.asList("id", "name", "email");
        WithCteSqlGenerator sqlGenerator = new WithCteSqlGenerator();
        WideTableIncrementalUpdater cteUpdater = new WideTableIncrementalUpdater(
                "id", "users", fields, sqlGenerator, mockDuckDbOperator);

        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(456));

        List<Map<String, Object>> afterRows = Arrays.asList(
                createRow(456, "John", "john@example.com")
        );

        List<Map<String, Object>> queryResult = Arrays.asList(
                createRow(456, "John", "john@example.com")
        );
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<WideTableCdcEvent> result = cteUpdater.updateWideTable(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        assertEquals(2, result.size());
        assertEquals(DELETE, result.get(0).getOpType());
        assertEquals(123, result.get(0).getPrimaryKey());
        assertEquals(INSERT, result.get(1).getOpType());
        assertEquals(456, result.get(1).getPrimaryKey());

        verify(mockDuckDbOperator).executeQuery(argThat(sql ->
                sql != null && sql.contains("WITH users AS") && sql.contains("VALUES")
        ));
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createRow(Object id, String name, String email) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("email", email);
        return row;
    }

    private List<WideTableCdcEvent> filterByOp(List<WideTableCdcEvent> events, WideTableCdcEvent.OpType opType) {
        List<WideTableCdcEvent> filtered = new ArrayList<>();
        for (WideTableCdcEvent event : events) {
            if (event.getOpType() == opType) {
                filtered.add(event);
            }
        }
        return filtered;
    }
}
