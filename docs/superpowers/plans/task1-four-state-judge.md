# Task 1: Create FourStateJudge Class

## Task Description

### Files:
- Create: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudge.java`
- Create: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeTest.java`

### Step 1: Write failing tests - FourStateJudge basic operations

Create test file at: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeTest.java`

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

### Step 2: Run tests to verify they fail

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=FourStateJudgeTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: FAIL with "FourStateJudge not defined"

### Step 3: Implement FourStateJudge

Create implementation file at: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudge.java`

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

### Step 4: Run tests to verify they pass

Run: `cd /Users/hj/workspace/tapdata && mvn test -Dtest=FourStateJudgeTest -pl iengine/iengine-app -q 2>&1 | tail -5`
Expected: All tests PASS

### Step 5: Commit

```bash
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudge.java \
        iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeTest.java
git commit -m "feat: add FourStateJudge for INSERT/UPDATE/DELETE/SKIP determination with TapdataEvent output"
```

## Context

This is Task 1 of a 4-task plan to create FourStateJudge class. FourStateJudge decouples four-state judgment logic from WideTableIncrementalUpdater and outputs standard TapdataEvent CDC events.

**Architecture context:**
- Current system: WideTableIncrementalUpdater handles SQL execution + event generation
- New system: FourStateJudge handles judgment logic, WideTableIncrementalUpdater delegates to it
- Output: TapdataEvent (Tapdata standard event type) instead of WideTableCdcEvent (custom type)

**Dependencies:**
- TapdataEvent: `com.tapdata.entity.TapdataEvent`
- TapInsertRecordEvent: `io.tapdata.event.TapInsertRecordEvent`
- TapUpdateRecordEvent: `io.tapdata.event.TapUpdateRecordEvent`
- TapDeleteRecordEvent: `io.tapdata.event.TapDeleteRecordEvent`

**Existing code patterns:**
- Located in: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/`
- Test location: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/`
- Uses JUnit 5 with Mockito
- Follows TDD: tests first, then implementation

## Before You Begin

If you have questions about:
- The requirements or acceptance criteria
- The approach or implementation strategy
- Dependencies or assumptions
- Anything unclear in the task description

**Ask them now.** Raise any concerns before starting work.

## Your Job

Once you're clear on requirements:
1. Implement exactly what the task specifies
2. Write tests (following TDD if task says to)
3. Verify implementation works
4. Commit your work
5. Self-review (see below)
6. Report back

Work from: /Users/hj/workspace/tapdata

**While you work:** If you encounter something unexpected or unclear, **ask questions**.
It's always OK to pause and clarify. Don't guess or make assumptions.

## Code Organization

You reason best about code you can hold in context at once, and your edits are
reliable when files are focused. Keep this in mind:
- Follow the file structure defined in the plan
- Each file should have one clear responsibility with a well-defined interface
- If a file you're creating is growing beyond the plan's intent, stop and report
  it as DONE_WITH_CONCERNS — don't split files on your own without plan guidance
- If an existing file you're modifying is already large or tangled, work carefully
  and note it as a concern in your report
- In existing codebases, follow established patterns. Improve code you're touching
  the way a good developer would, but don't restructure things outside your task.

## When You're in Over Your Head

It is always OK to stop and say "this is too hard for me." Bad work is worse than
no work. You will not be penalized for escalating.

**STOP and escalate when:**
- The task requires architectural decisions with multiple valid approaches
- You need to understand code beyond what was provided and can't find clarity
- You feel uncertain about whether your approach is correct
- The task involves restructuring existing code in ways the plan didn't anticipate
- You've been reading file after file trying to understand the system without progress

**How to escalate:** Report back with status BLOCKED or NEEDS_CONTEXT. Describe
specifically what you're stuck on, what you've tried, and what kind of help you need.
The controller can provide more context, re-dispatch with a more capable model,
or break the task into smaller pieces.

## Before Reporting Back: Self-Review

Review your work with fresh eyes. Ask yourself:

**Completeness:**
- Did I fully implement everything in the spec?
- Did I miss any requirements?
- Are there edge cases I didn't handle?

**Quality:**
- Is this my best work?
- Are names clear and accurate (match what things do, not how they work)?
- Is the code clean and maintainable?

**Discipline:**
- Did I avoid overbuilding (YAGNI)?
- Did I only build what was requested?
- Did I follow existing patterns in the codebase?

**Testing:**
- Do tests actually verify behavior (not just mock behavior)?
- Did I follow TDD if required?
- Are tests comprehensive?

If you find issues during self-review, fix them now before reporting.

## Report Format

When done, report:
- **Status:** DONE | DONE_WITH_CONCERNS | BLOCKED | NEEDS_CONTEXT
- What you implemented (or what you attempted, if blocked)
- What you tested and test results
- Files changed
- Self-review findings (if any)
- Any issues or concerns

Use DONE_WITH_CONCERNS if you completed the work but have doubts about correctness.
Use BLOCKED if you cannot complete the task. Use NEEDS_CONTEXT if you need
information that wasn't provided. Never silently produce work you're unsure about.
