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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * 集成测试 - FourStateJudge 端到端流程
 * 验证 AffectedKeyCalculator + WithCteSqlGenerator + WideTableIncrementalUpdater + FourStateJudge 联合工作
 */
@ExtendWith(MockitoExtension.class)
class FourStateJudgeIntegrationTest {

    @Mock
    private DuckDbOperator mockDuckDbOperator;

    private WideTableIncrementalUpdater updater;

    @BeforeEach
    void setUp() {
        List<String> fields = Arrays.asList("id", "name", "email");
        WithCteSqlGenerator sqlGenerator = new WithCteSqlGenerator();
        updater = new WideTableIncrementalUpdater("users", "id",
                "SELECT id, name, email FROM users",
                fields, sqlGenerator, mockDuckDbOperator);
    }

    @Test
    void testEndToEnd_FourStateJudgeFlow() throws SQLException, IOException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Arrays.asList(123, 789));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>(Arrays.asList(456, 789));

        List<Map<String, Object>> afterRows = Arrays.asList(
                createRow(456, "John", "john@example.com"),
                createRow(789, "Jane Updated", "jane@example.com")
        );

        List<Map<String, Object>> queryResult = new ArrayList<>(afterRows);
        when(mockDuckDbOperator.executeQuery(anyString())).thenReturn(queryResult);

        List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

        assertEquals(3, events.size());

        // 验证 DELETE 事件 (123 在 before 中但不在 after 中)
        List<TapdataEvent> deleteEvents = filterByType(events, TapDeleteRecordEvent.class);
        assertEquals(1, deleteEvents.size());
        TapDeleteRecordEvent deleteEvent = (TapDeleteRecordEvent) deleteEvents.get(0).getTapEvent();
        assertEquals("users", deleteEvent.getTableId());

        // 验证 INSERT 事件 (456 不在 before 中)
        List<TapdataEvent> insertEvents = filterByType(events, TapInsertRecordEvent.class);
        assertEquals(1, insertEvents.size());
        TapInsertRecordEvent insertEvent = (TapInsertRecordEvent) insertEvents.get(0).getTapEvent();
        assertEquals("users", insertEvent.getTableId());
        assertEquals("John", insertEvent.getAfter().get("name"));

        // 验证 UPDATE 事件 (789 在 before 和 after 中)
        List<TapdataEvent> updateEvents = filterByType(events, TapUpdateRecordEvent.class);
        assertEquals(1, updateEvents.size());
        TapUpdateRecordEvent updateEvent = (TapUpdateRecordEvent) updateEvents.get(0).getTapEvent();
        assertEquals("users", updateEvent.getTableId());
        assertEquals("Jane Updated", updateEvent.getAfter().get("name"));
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> createRow(Object id, String name, String email) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", name);
        row.put("email", email);
        return row;
    }

    private List<TapdataEvent> filterByType(List<TapdataEvent> events, Class<?> eventType) {
        List<TapdataEvent> filtered = new ArrayList<>();
        for (TapdataEvent event : events) {
            if (eventType.isInstance(event.getTapEvent())) {
                filtered.add(event);
            }
        }
        return filtered;
    }
}
