# FourStateJudge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create FourStateJudge class to decouple four-state judgment logic and output standard TapdataEvent CDC events

**Architecture:** FourStateJudge receives before primary key set and after data rows, performs INSERT/UPDATE/DELETE/SKIP judgment, and outputs standard TapdataEvent (wrapping TapInsertRecordEvent/TapUpdateRecordEvent/TapDeleteRecordEvent). WideTableIncrementalUpdater integrates FourStateJudge for event generation.

**Tech Stack:** Java 17, JUnit 5, Mockito, Tapdata TapEvent API

---

## File Structure

| File | Operation | Responsibility |
|------|-----------|----------------|
| `FourStateJudge.java` | Create | Four-state judgment logic, outputs TapdataEvent |
| `FourStateJudgeTest.java` | Create | Unit tests for FourStateJudge |
| `WideTableIncrementalUpdater.java` | Modify | Integrate FourStateJudge, deprecate WideTableCdcEvent |
| `WideTableIncrementalUpdaterTest.java` | Modify | Update tests for FourStateJudge integration |
| `WideTableCdcEvent.java` | Modify | Mark as @Deprecated |

---

### Task 1: Create FourStateJudge Class

**Files:**
- Create: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudge.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeTest.java`

- [ ] **Step 1: Write failing tests - FourStateJudge basic operations**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.event.TapInsertRecordEvent;
import io.tapdata.event.TapUpdateRecordEvent;
import io.tapdata.event.TapDeleteRecordEvent;
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

        assertEquals(3, events.size());

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=FourStateJudgeTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: FAIL with "FourStateJudge not defined"

- [ ] **Step 3: Implement FourStateJudge**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.event.TapInsertRecordEvent;
import io.tapdata.event.TapUpdateRecordEvent;
import io.tapdata.event.TapDeleteRecordEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 四态判断器
 * 根据 before/after 数据判断 INSERT/UPDATE/DELETE/SKIP 操作
 * 输出标准 TapdataEvent 事件
 */
public class FourStateJudge {

    private static final Logger logger = LoggerFactory.getLogger(FourStateJudge.class);

    private final String tableId;
    private final String wideTablePrimaryKey;

    public FourStateJudge(String tableId, String wideTablePrimaryKey) {
        this.tableId = tableId;
        this.wideTablePrimaryKey = wideTablePrimaryKey;
    }

    /**
     * 四态判断
     * @param beforePks before SQL 返回的主键集合
     * @param afterData after SQL 返回的完整数据
     * @return TapdataEvent 事件列表
     */
    public List<TapdataEvent> judge(Set<Object> beforePks, List<Map<String, Object>> afterData) {
        List<TapdataEvent> events = new ArrayList<>();

        Set<Object> beforePkSet = beforePks != null ? beforePks : Collections.emptySet();
        List<Map<String, Object>> afterDataList = afterData != null ? afterData : Collections.emptyList();

        Set<Object> afterPks = extractPrimaryKeys(afterDataList);

        // 有旧无新 → DELETE
        for (Object pk : beforePkSet) {
            if (!afterPks.contains(pk)) {
                TapDeleteRecordEvent deleteEvent = TapDeleteRecordEvent.create()
                        .table(tableId)
                        .before(Collections.singletonMap(wideTablePrimaryKey, pk));
                TapdataEvent tapdataEvent = new TapdataEvent();
                tapdataEvent.setTapEvent(deleteEvent);
                events.add(tapdataEvent);
                logger.debug("Four-state judge: DELETE pk={}", pk);
            }
        }

        // 无旧有新 → INSERT / 新旧都有 → UPDATE
        for (Map<String, Object> row : afterDataList) {
            Object pk = row.get(wideTablePrimaryKey);
            if (pk == null) {
                logger.warn("Wide table primary key '{}' not found in row: {}", wideTablePrimaryKey, row);
                continue;
            }
            if (beforePkSet.contains(pk)) {
                TapUpdateRecordEvent updateEvent = TapUpdateRecordEvent.create()
                        .table(tableId)
                        .after(row);
                TapdataEvent tapdataEvent = new TapdataEvent();
                tapdataEvent.setTapEvent(updateEvent);
                events.add(tapdataEvent);
                logger.debug("Four-state judge: UPDATE pk={}", pk);
            } else {
                TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
                        .table(tableId)
                        .after(row);
                TapdataEvent tapdataEvent = new TapdataEvent();
                tapdataEvent.setTapEvent(insertEvent);
                events.add(tapdataEvent);
                logger.debug("Four-state judge: INSERT pk={}", pk);
            }
        }

        logger.debug("Four-state judge result: {} events (beforePks={}, afterPks={})",
                events.size(), beforePkSet.size(), afterPks.size());

        return events;
    }

    private Set<Object> extractPrimaryKeys(List<Map<String, Object>> data) {
        Set<Object> pks = new HashSet<>();
        for (Map<String, Object> row : data) {
            Object pk = row.get(wideTablePrimaryKey);
            if (pk != null) {
                pks.add(pk);
            }
        }
        return pks;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=FourStateJudgeTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudge.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeTest.java
git commit -m "feat: add FourStateJudge for INSERT/UPDATE/DELETE/SKIP determination with TapdataEvent output"
```

---

### Task 2: Integrate FourStateJudge into WideTableIncrementalUpdater

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java`
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableCdcEvent.java`

- [ ] **Step 1: Mark WideTableCdcEvent as @Deprecated**

```java
/**
 * 宽表 CDC 事件
 * 表示宽表的一次变更操作（INSERT/UPDATE/DELETE）
 * @deprecated Use FourStateJudge with TapdataEvent instead
 */
@Deprecated
public class WideTableCdcEvent {
    // ... existing code ...
}
```

- [ ] **Step 2: Add FourStateJudge dependency to WideTableIncrementalUpdater**

Read current file: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`

Add FourStateJudge field and update constructors:

```java
    private final String wideTablePrimaryKey;
    private final String querySql;
    private final List<String> fields;
    private final WithCteSqlGenerator withCteSqlGenerator;
    private final DuckDbOperator duckDbOperator;
    private final FourStateJudge fourStateJudge;

    /**
     * 构造函数（向后兼容，使用 IN 子句方式）
     * @deprecated Use constructor with WithCteSqlGenerator
     */
    @Deprecated
    public WideTableIncrementalUpdater(String wideTablePrimaryKey, String querySql,
                                       List<String> fields, DuckDbOperator duckDbOperator) {
        this(wideTablePrimaryKey, querySql, fields, new WithCteSqlGenerator(), duckDbOperator);
    }

    /**
     * 构造函数（使用 WITH CTE 方式）
     */
    public WideTableIncrementalUpdater(String wideTablePrimaryKey, String querySql,
                                       List<String> fields, WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.querySql = querySql;
        this.fields = fields;
        this.withCteSqlGenerator = withCteSqlGenerator;
        this.duckDbOperator = duckDbOperator;
        this.fourStateJudge = new FourStateJudge("users", wideTablePrimaryKey);
    }
```

- [ ] **Step 3: Add new updateWideTable method returning TapdataEvent**

Add after existing `updateWideTable` methods:

```java
    /**
     * 批量更新宽表（使用 FourStateJudge 输出 TapdataEvent）
     * @param affectedBeforeKeys before 受影响主键集合（用于 DELETE 宽表记录）
     * @param affectedAfterKeys after 受影响主键集合（用于 INSERT/UPDATE 宽表记录）
     * @param afterRows after 数据行（从 CDC 事件提取）
     * @param tableName 源表名（用于 WITH CTE 临时表名）
     * @return TapdataEvent 事件列表
     */
    public List<TapdataEvent> updateWideTableAsTapdataEvents(Set<Object> affectedBeforeKeys,
                                                              Set<Object> affectedAfterKeys,
                                                              List<Map<String, Object>> afterRows,
                                                              String tableName) throws SQLException {
        // 1. 计算纯 DELETE 的主键（在 before 中但不在 after 中）
        Set<Object> pureDeleteKeys = new LinkedHashSet<>(affectedBeforeKeys);
        pureDeleteKeys.removeAll(affectedAfterKeys);

        // 2. 使用 WITH CTE 执行 after 查询
        List<Map<String, Object>> results = Collections.emptyList();
        if (afterRows != null && !afterRows.isEmpty()) {
            String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, fields);
            results = duckDbOperator.executeQuery(afterSql);
        }

        // 3. 使用 FourStateJudge 进行四态判断
        return fourStateJudge.judge(pureDeleteKeys, results);
    }
```

- [ ] **Step 4: Run all WideTableIncrementalUpdater tests**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=WideTableIncrementalUpdaterTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: All tests PASS (existing tests should still work via deprecated constructor)

- [ ] **Step 5: Commit**

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java \
        iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableCdcEvent.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java
git commit -m "feat: integrate FourStateJudge into WideTableIncrementalUpdater, deprecate WideTableCdcEvent"
```

---

### Task 3: Integration Test - FourStateJudge End-to-End Flow

**Files:**
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeIntegrationTest.java`

- [ ] **Step 1: Write integration test**

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.event.TapInsertRecordEvent;
import io.tapdata.event.TapUpdateRecordEvent;
import io.tapdata.event.TapDeleteRecordEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
        updater = new WideTableIncrementalUpdater("id",
                "SELECT id, name, email FROM users",
                fields, sqlGenerator, mockDuckDbOperator);
    }

    @Test
    void testEndToEnd_FourStateJudgeFlow() throws SQLException {
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
```

- [ ] **Step 2: Run integration test**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=FourStateJudgeIntegrationTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeIntegrationTest.java
git commit -m "test: add end-to-end integration test for FourStateJudge flow"
```

---

### Task 4: Run All Tests and Final Cleanup

**Files:**
- No file changes expected

- [ ] **Step 1: Run all duckdb tests**

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest="AffectedKeyCalculatorTest,SchemaResolverTest,WideTableIncrementalUpdaterTest,BatchWideTableUpdateIntegrationTest,WithCteSqlGeneratorTest,WithCteIntegrationTest,FourStateJudgeTest,FourStateJudgeIntegrationTest" -pl iengine/iengine-app --fail-at-end 2>&1 | grep -E "(Tests run|BUILD|FAILURE)" | tail -5`
Expected: All tests PASS (100+ tests)

- [ ] **Step 2: Verify no regression**

Run: `cd /Users/hj/workspace/tapdata && git status`
Expected: Clean working tree (all changes committed)

- [ ] **Step 3: Final commit if needed**

```bash
cd /Users/hj/workspace/tapdata
git log --oneline -5
```

---

## Self-Review

**1. Spec coverage:**
- FourStateJudge class ✓ (Task 1)
- Four-state judgment logic (INSERT/UPDATE/DELETE/SKIP) ✓ (Task 1)
- TapdataEvent output ✓ (Task 1)
- Integration with WideTableIncrementalUpdater ✓ (Task 2)
- WideTableCdcEvent deprecation ✓ (Task 2)
- End-to-end flow test ✓ (Task 3)
- Error handling (null inputs, missing primary key) ✓ (Task 1)

**2. Placeholder scan:** No TBD/TODO found. All steps contain actual code.

**3. Type consistency:**
- `TapdataEvent` used consistently as output type
- `TapInsertRecordEvent`, `TapUpdateRecordEvent`, `TapDeleteRecordEvent` for specific event types
- `FourStateJudge` injected via constructor
- Method signatures match across tasks
- `List<Map<String, Object>>` for rows, `Set<Object>` for keys

**4. Scope check:** Focused on FourStateJudge creation and integration. No unrelated refactoring.
