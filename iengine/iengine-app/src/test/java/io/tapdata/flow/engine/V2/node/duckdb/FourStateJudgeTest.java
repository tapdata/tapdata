package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class FourStateJudgeTest {

    private FourStateJudge judge;

    @BeforeEach
    void setUp() {
        judge = new FourStateJudge("users", "id");
    }

    @Test
    void testJudge_Insert() {
        Set<Object> beforePks = new HashSet<>();
        List<Map<String, Object>> afterData = new ArrayList<>();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("name", "John");
        afterData.add(row);

        List<TapdataEvent> events = judge.judge(beforePks, afterData);

        assertEquals(1, events.size());
        TapdataEvent event = events.get(0);
        assertTrue(event.getTapEvent() instanceof TapInsertRecordEvent);
        TapInsertRecordEvent insertEvent = (TapInsertRecordEvent) event.getTapEvent();
        assertEquals("users", insertEvent.getTableId());
        assertEquals("John", insertEvent.getAfter().get("name"));
    }

    @Test
    void testJudge_Delete() {
        Set<Object> beforePks = new HashSet<>(Collections.singleton(1));
        List<Map<String, Object>> afterData = new ArrayList<>();

        List<TapdataEvent> events = judge.judge(beforePks, afterData);

        assertEquals(1, events.size());
        TapdataEvent event = events.get(0);
        assertTrue(event.getTapEvent() instanceof TapDeleteRecordEvent);
        TapDeleteRecordEvent deleteEvent = (TapDeleteRecordEvent) event.getTapEvent();
        assertEquals("users", deleteEvent.getTableId());
    }

    @Test
    void testJudge_Update() {
        Set<Object> beforePks = new HashSet<>(Collections.singleton(1));
        List<Map<String, Object>> afterData = new ArrayList<>();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("name", "John Updated");
        afterData.add(row);

        List<TapdataEvent> events = judge.judge(beforePks, afterData);

        assertEquals(1, events.size());
        TapdataEvent event = events.get(0);
        assertTrue(event.getTapEvent() instanceof TapUpdateRecordEvent);
        TapUpdateRecordEvent updateEvent = (TapUpdateRecordEvent) event.getTapEvent();
        assertEquals("users", updateEvent.getTableId());
        assertEquals("John Updated", updateEvent.getAfter().get("name"));
    }

    @Test
    void testJudge_Skip() {
        Set<Object> beforePks = new HashSet<>();
        List<Map<String, Object>> afterData = new ArrayList<>();

        List<TapdataEvent> events = judge.judge(beforePks, afterData);

        assertTrue(events.isEmpty());
    }

    @Test
    void testJudge_MixedOperations() {
        Set<Object> beforePks = new HashSet<>(Arrays.asList(1, 2, 3));
        List<Map<String, Object>> afterData = new ArrayList<>();

        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("name", "Updated");
        afterData.add(row1);

        Map<String, Object> row4 = new LinkedHashMap<>();
        row4.put("id", 4);
        row4.put("name", "New");
        afterData.add(row4);

        List<TapdataEvent> events = judge.judge(beforePks, afterData);

        assertEquals(4, events.size());

        Map<Object, String> pkToOp = new HashMap<>();
        for (TapdataEvent event : events) {
            if (event.getTapEvent() instanceof TapInsertRecordEvent) {
                pkToOp.put(((TapInsertRecordEvent) event.getTapEvent()).getAfter().get("id"), "INSERT");
            } else if (event.getTapEvent() instanceof TapUpdateRecordEvent) {
                pkToOp.put(((TapUpdateRecordEvent) event.getTapEvent()).getAfter().get("id"), "UPDATE");
            } else if (event.getTapEvent() instanceof TapDeleteRecordEvent) {
                pkToOp.put(((TapDeleteRecordEvent) event.getTapEvent()).getBefore().get("id"), "DELETE");
            }
        }

        assertEquals("UPDATE", pkToOp.get(1));
        assertEquals("DELETE", pkToOp.get(2));
        assertEquals("DELETE", pkToOp.get(3));
        assertEquals("INSERT", pkToOp.get(4));
    }

    @Test
    void testJudge_NullBeforePks() {
        List<Map<String, Object>> afterData = new ArrayList<>();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        afterData.add(row);

        List<TapdataEvent> events = judge.judge(null, afterData);

        assertEquals(1, events.size());
        assertTrue(events.get(0).getTapEvent() instanceof TapInsertRecordEvent);
    }

    @Test
    void testJudge_NullAfterData() {
        Set<Object> beforePks = new HashSet<>(Collections.singleton(1));

        List<TapdataEvent> events = judge.judge(beforePks, null);

        assertEquals(1, events.size());
        assertTrue(events.get(0).getTapEvent() instanceof TapDeleteRecordEvent);
    }

    @Test
    void testJudge_MissingPrimaryKey() {
        Set<Object> beforePks = new HashSet<>();
        List<Map<String, Object>> afterData = new ArrayList<>();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("name", "John"); // Missing 'id' field

        List<TapdataEvent> events = judge.judge(beforePks, afterData);

        assertTrue(events.isEmpty());
    }
}
