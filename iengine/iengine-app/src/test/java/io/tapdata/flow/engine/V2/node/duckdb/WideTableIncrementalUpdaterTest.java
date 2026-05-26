package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WideTableIncrementalUpdaterTest {

    @Mock
    private DuckDbOperator mockDuckDbOperator;

    private WideTableIncrementalUpdater updater;

    @BeforeEach
    void setUp() {
        List<String> fields = Arrays.asList("id", "name", "email");
        updater = new WideTableIncrementalUpdater(
                "wide_table", "id",
                "SELECT id, name, email FROM users",
                fields, new WithCteSqlGenerator(), mockDuckDbOperator, false);
    }

    // ==================== 非事务模式测试 ====================

    @Test
    void testUpdateWideTableAsTapdataEvents_deleteAndInsert() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Arrays.asList(123, 789));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Arrays.asList(456, 789));
        List<Map<String, Object>> afterRows = Arrays.asList(
                createRow(456, "John", "john@example.com"),
                createRow(789, "Jane Updated", "jane@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        // 1 DELETE (123), 1 INSERT (456), 1 UPDATE (789)
        assertEquals(3, result.size());
        assertEquals(1, countByType(result, TapDeleteRecordEvent.class));
        assertEquals(1, countByType(result, TapInsertRecordEvent.class));
        assertEquals(1, countByType(result, TapUpdateRecordEvent.class));
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_primaryKeyUpdate() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(456));
        List<Map<String, Object>> afterRows = Collections.singletonList(
                createRow(456, "John", "john@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        assertEquals(2, result.size());
        assertEquals(1, countByType(result, TapDeleteRecordEvent.class));
        assertEquals(1, countByType(result, TapInsertRecordEvent.class));
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_onlyDelete() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Arrays.asList(123, 456));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, Collections.emptyList(), "users");

        assertEquals(2, result.size());
        assertEquals(2, countByType(result, TapDeleteRecordEvent.class));
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_onlyInsert() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Arrays.asList(123, 456));
        List<Map<String, Object>> afterRows = Arrays.asList(
                createRow(123, "John", "john@example.com"),
                createRow(456, "Jane", "jane@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        assertEquals(2, result.size());
        assertEquals(2, countByType(result, TapInsertRecordEvent.class));
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_emptyBoth() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, Collections.emptyList(), "users");

        assertTrue(result.isEmpty());
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_noChange() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(123));
        List<Map<String, Object>> afterRows = Collections.singletonList(
                createRow(123, "John", "john@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        // FourStateJudge 产生 UPDATE 事件（在 before 和 after 中都有）
        assertEquals(1, result.size());
        assertEquals(1, countByType(result, TapUpdateRecordEvent.class));
    }

    // ==================== 事务模式测试 ====================

    @Test
    void testUpdateWideTableAsTapdataEvents_TransactionMode_CallsExecuteInTransaction() throws SQLException, IOException {
        WideTableIncrementalUpdater transactionUpdater = new WideTableIncrementalUpdater(
                "wide_table", "id",
                "SELECT id, name, email FROM users",
                Arrays.asList("id", "name", "email"),
                new WithCteSqlGenerator(), mockDuckDbOperator, true);

        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(456));
        List<Map<String, Object>> afterRows = Collections.singletonList(
                createRow(456, "John", "john@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);
        doAnswer(invocation -> {
            DuckDbOperator.ThrowingConsumer action = invocation.getArgument(0);
            action.accept();
            return null;
        }).when(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));

        List<TapdataEvent> result = transactionUpdater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        verify(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));
        assertEquals(2, result.size());
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_TransactionMode_RollbackOnError() throws SQLException, IOException {
        WideTableIncrementalUpdater transactionUpdater = new WideTableIncrementalUpdater(
                "wide_table", "id",
                "SELECT id, name, email FROM users",
                Arrays.asList("id", "name", "email"),
                new WithCteSqlGenerator(), mockDuckDbOperator, true);

        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        List<Map<String, Object>> afterRows = Collections.singletonList(
                createRow(456, "John", "john@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString()))
                .thenThrow(new SQLException("Query failed"));

        doAnswer(invocation -> {
            DuckDbOperator.ThrowingConsumer action = invocation.getArgument(0);
            action.accept();
            return null;
        }).when(mockDuckDbOperator).executeInTransaction(any(DuckDbOperator.ThrowingConsumer.class));

        assertThrows(SQLException.class, () -> transactionUpdater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, new LinkedHashSet<>(), afterRows, "users"));
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_NonTransactionMode_DoesNotCallExecuteInTransaction() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        List<Map<String, Object>> afterRows = Collections.singletonList(
                createRow(456, "John", "john@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, new LinkedHashSet<>(), afterRows, "users");

        verify(mockDuckDbOperator, never()).executeInTransaction(any());
    }

    // ==================== ChangelogListener 测试 ====================

    @Test
    void testUpdateWideTableAsTapdataEvents_ChangelogListener_ReceivesEvents() throws SQLException, IOException {
        AtomicInteger eventCount = new AtomicInteger(0);
        updater.addChangelogListener(event -> eventCount.incrementAndGet());

        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(456));
        List<Map<String, Object>> afterRows = Collections.singletonList(
                createRow(456, "John", "john@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        assertEquals(2, result.size());
        assertEquals(2, eventCount.get());
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_ChangelogListener_ExceptionDoesNotBreakFlow() throws SQLException, IOException {
        updater.addChangelogListener(event -> {
            throw new RuntimeException("Listener error");
        });
        AtomicInteger successCount = new AtomicInteger(0);
        updater.addChangelogListener(event -> successCount.incrementAndGet());

        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        List<Map<String, Object>> afterRows = Collections.singletonList(
                createRow(456, "John", "john@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, new LinkedHashSet<>(), afterRows, "users");

        // 第一个 listener 异常不应影响第二个 listener
        // 主键更新场景产生 2 个事件 (DELETE + INSERT)，所以 successCount = 2
        assertEquals(2, successCount.get());
        assertEquals(2, result.size());
    }

    // ==================== WITH CTE 测试 ====================

    @Test
    void testUpdateWideTableAsTapdataEvents_WithCteIntegration() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Collections.singletonList(123));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Collections.singletonList(456));
        List<Map<String, Object>> afterRows = Arrays.asList(
                createRow(456, "John", "john@example.com")
        );

        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(afterRows);

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        assertEquals(2, result.size());

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

    private long countByType(List<TapdataEvent> events, Class<?> eventType) {
        return events.stream()
                .filter(e -> eventType.isInstance(e.getTapEvent()))
                .count();
    }
}
