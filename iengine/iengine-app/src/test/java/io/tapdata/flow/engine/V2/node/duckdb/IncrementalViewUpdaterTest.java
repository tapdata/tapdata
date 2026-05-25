package io.tapdata.flow.engine.V2.node.duckdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class IncrementalViewUpdaterTest {

    private DuckDbOperator mockDuckDbOperator;
    private IncrementalViewUpdater incrementalViewUpdater;
    private AtomicInteger changelogCallCount;

    @BeforeEach
    void setUp() {
        mockDuckDbOperator = Mockito.mock(DuckDbOperator.class);
        changelogCallCount = new AtomicInteger(0);

        incrementalViewUpdater = new IncrementalViewUpdater(
                "wide_table",
                "id",
                "SELECT u.id, u.name, o.order_id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
                true,
                mockDuckDbOperator
        );
    }

    @Test
    void testUpdateWideTable_EmptyAffectedKeys() throws Exception {
        assertDoesNotThrow(() -> incrementalViewUpdater.updateWideTable(Collections.emptySet()));

        verify(mockDuckDbOperator, never()).queryForMap(anyString(), anyString());
    }

    @Test
    void testUpdateWideTable_NullAffectedKeys() throws Exception {
        assertDoesNotThrow(() -> incrementalViewUpdater.updateWideTable(null));

        verify(mockDuckDbOperator, never()).queryForMap(anyString(), anyString());
    }

    @Test
    void testUpdateWideTable_InsertNewRow() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Collections.singletonList(1L));

        Map<Object, Map<String, Object>> oldValues = new HashMap<>();

        Map<Object, Map<String, Object>> newValues = new HashMap<>();
        Map<String, Object> newRow = new HashMap<>();
        newRow.put("id", 1L);
        newRow.put("name", "Alice");
        newRow.put("order_id", "ORD001");
        newRow.put("amount", 100.0);
        newValues.put(1L, newRow);

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        incrementalViewUpdater.updateWideTable(affectedKeys);

        verify(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));
    }

    @Test
    void testUpdateWideTable_UpdateExistingRow() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Collections.singletonList(2L));

        Map<String, Object> oldRow = new HashMap<>();
        oldRow.put("id", 2L);
        oldRow.put("name", "Bob");
        oldRow.put("order_id", "ORD002");
        oldRow.put("amount", 200.0);
        Map<Object, Map<String, Object>> oldValues = new HashMap<>();
        oldValues.put(2L, oldRow);

        Map<String, Object> newRow = new HashMap<>();
        newRow.put("id", 2L);
        newRow.put("name", "Bob Updated");
        newRow.put("order_id", "ORD002");
        newRow.put("amount", 250.0);
        Map<Object, Map<String, Object>> newValues = new HashMap<>();
        newValues.put(2L, newRow);

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        incrementalViewUpdater.updateWideTable(affectedKeys);

        verify(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));
    }

    @Test
    void testUpdateWideTable_DeleteRow() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Collections.singletonList(3L));

        Map<String, Object> oldRow = new HashMap<>();
        oldRow.put("id", 3L);
        oldRow.put("name", "Charlie");
        Map<Object, Map<String, Object>> oldValues = new HashMap<>();
        oldValues.put(3L, oldRow);

        Map<Object, Map<String, Object>> newValues = new HashMap<>();

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        incrementalViewUpdater.updateWideTable(affectedKeys);

        verify(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));
    }

    @Test
    void testUpdateWideTable_NoChange() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Collections.singletonList(4L));

        Map<String, Object> oldRow = new HashMap<>();
        oldRow.put("id", 4L);
        oldRow.put("name", "David");
        Map<Object, Map<String, Object>> oldValues = new HashMap<>();
        oldValues.put(4L, oldRow);

        Map<String, Object> newRow = new HashMap<>();
        newRow.put("id", 4L);
        newRow.put("name", "David");
        Map<Object, Map<String, Object>> newValues = new HashMap<>();
        newValues.put(4L, newRow);

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        incrementalViewUpdater.updateWideTable(affectedKeys);

        verify(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));
    }

    @Test
    void testUpdateWideTable_MultipleAffectedKeys() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Arrays.asList(1L, 2L, 3L));

        Map<String, Object> oldRow1 = new HashMap<>();
        oldRow1.put("id", 1L);
        oldRow1.put("name", "Alice");
        Map<String, Object> oldRow2 = new HashMap<>();
        oldRow2.put("id", 2L);
        oldRow2.put("name", "Bob");
        Map<String, Object> oldRow3 = new HashMap<>();
        oldRow3.put("id", 3L);
        oldRow3.put("name", "Charlie");

        Map<Object, Map<String, Object>> oldValues = new HashMap<>();
        oldValues.put(1L, oldRow1);
        oldValues.put(2L, oldRow2);
        oldValues.put(3L, oldRow3);

        Map<String, Object> newRow1 = new HashMap<>();
        newRow1.put("id", 1L);
        newRow1.put("name", "Alice Updated");
        Map<String, Object> newRow2 = new HashMap<>();
        newRow2.put("id", 2L);
        newRow2.put("name", "Bob");
        Map<Object, Map<String, Object>> newValues = new HashMap<>();
        newValues.put(1L, newRow1);
        newValues.put(2L, newRow2);

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        incrementalViewUpdater.updateWideTable(affectedKeys);

        verify(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));
    }

    @Test
    void testUpdateWideTable_ChangelogDisabled() throws Exception {
        IncrementalViewUpdater noChangelogUpdater = new IncrementalViewUpdater(
                "wide_table",
                "id",
                "SELECT * FROM users",
                false,
                mockDuckDbOperator
        );

        Set<Object> affectedKeys = new HashSet<>(Collections.singletonList(1L));

        Map<Object, Map<String, Object>> oldValues = new HashMap<>();

        Map<String, Object> newRow = new HashMap<>();
        newRow.put("id", 1L);
        newRow.put("name", "Alice");
        Map<Object, Map<String, Object>> newValues = new HashMap<>();
        newValues.put(1L, newRow);

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        noChangelogUpdater.updateWideTable(affectedKeys);
    }

    @Test
    void testUpdateWideTable_TransactionRollbackOnError() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Collections.singletonList(1L));

        reset(mockDuckDbOperator);
        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenThrow(new SQLException("Query failed"));

        doAnswer(invocation -> {
            DuckDbOperator.ThrowingConsumer action = invocation.getArgument(0);
            action.accept();
            return null;
        }).when(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));

        assertThrows(Exception.class, () -> incrementalViewUpdater.updateWideTable(affectedKeys));
    }

    @Test
    void testAddChangelogListener() {
        AtomicInteger customCallCount = new AtomicInteger(0);
        incrementalViewUpdater.addChangelogListener(event -> {
            customCallCount.incrementAndGet();
        });

        assertNotNull(incrementalViewUpdater);
    }

    @Test
    void testUpdateWideTable_ComplexRowData() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Collections.singletonList(5L));

        Map<String, Object> oldRow = new HashMap<>();
        oldRow.put("id", 5L);
        oldRow.put("name", "Eve");
        oldRow.put("age", 30);
        oldRow.put("is_active", true);
        oldRow.put("tags", Arrays.asList("user", "premium"));
        Map<Object, Map<String, Object>> oldValues = new HashMap<>();
        oldValues.put(5L, oldRow);

        Map<String, Object> newRow = new HashMap<>();
        newRow.put("id", 5L);
        newRow.put("name", "Eve Updated");
        newRow.put("age", 31);
        newRow.put("is_active", true);
        newRow.put("tags", Arrays.asList("user", "premium", "vip"));
        Map<Object, Map<String, Object>> newValues = new HashMap<>();
        newValues.put(5L, newRow);

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        assertDoesNotThrow(() -> incrementalViewUpdater.updateWideTable(affectedKeys));
    }

    @Test
    void testUpdateWideTable_EmptyRow() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Collections.singletonList(6L));

        Map<String, Object> oldRow = new HashMap<>();
        oldRow.put("id", 6L);
        Map<Object, Map<String, Object>> oldValues = new HashMap<>();
        oldValues.put(6L, oldRow);

        Map<String, Object> newRow = new HashMap<>();
        newRow.put("id", 6L);
        Map<Object, Map<String, Object>> newValues = new HashMap<>();
        newValues.put(6L, newRow);

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        incrementalViewUpdater.updateWideTable(affectedKeys);
    }

    @Test
    void testUpdateWideTable_DifferentKeyTypes() throws Exception {
        Set<Object> affectedKeys = new HashSet<>(Arrays.asList(100, "user_abc"));

        Map<Object, Map<String, Object>> oldValues = new HashMap<>();
        Map<Object, Map<String, Object>> newValues = new HashMap<>();

        Map<String, Object> newRow1 = new HashMap<>();
        newRow1.put("id", 100);
        newRow1.put("name", "User 100");
        newValues.put(100, newRow1);

        Map<String, Object> newRow2 = new HashMap<>();
        newRow2.put("id", "user_abc");
        newRow2.put("name", "User ABC");
        newValues.put("user_abc", newRow2);

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        assertDoesNotThrow(() -> incrementalViewUpdater.updateWideTable(affectedKeys));
    }

    @Test
    void testUpdateWideTable_LargeDataSet() throws Exception {
        Set<Object> affectedKeys = new HashSet<>();
        Map<Object, Map<String, Object>> newValues = new HashMap<>();

        for (long i = 1; i <= 100; i++) {
            affectedKeys.add(i);

            Map<String, Object> newRow = new HashMap<>();
            newRow.put("id", i);
            newRow.put("name", "User_" + i);
            newValues.put(i, newRow);
        }

        Map<Object, Map<String, Object>> oldValues = new HashMap<>();

        when(mockDuckDbOperator.queryForMap(anyString(), eq("id")))
                .thenReturn(oldValues)
                .thenReturn(newValues);

        assertDoesNotThrow(() -> incrementalViewUpdater.updateWideTable(affectedKeys));
    }
}
