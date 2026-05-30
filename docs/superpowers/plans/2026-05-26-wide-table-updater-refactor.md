# WideTableIncrementalUpdater Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重构 WideTableIncrementalUpdater 为单一方法，新增可选事务支持（真实更新宽表）和 ChangelogListener 回调。

**Architecture:** 删除所有旧方法，保留 `updateWideTableAsTapdataEvents` 作为唯一核心方法。通过构造函数 `enableTransaction` 参数控制行为：事务模式包裹 `executeInTransaction` + 真实更新宽表，非事务模式仅生成事件。新增 `ChangelogListener` 接口接收标准 `TapdataEvent`。

**Tech Stack:** Java, DuckDB, Mockito, JUnit 5, TapdataEvent

---

## File Structure

| 文件 | 操作 | 说明 |
|------|------|------|
| `WideTableIncrementalUpdater.java` | 修改 | 重构为单一方法 + 事务 + ChangelogListener |
| `WideTableIncrementalUpdaterTest.java` | 重写 | 全部测试用例适配新 API |
| `FourStateJudgeIntegrationTest.java` | 修改 | 适配新构造函数签名 |
| `WithCteIntegrationTest.java` | 修改 | 适配新 API |
| `BatchWideTableUpdateIntegrationTest.java` | 修改 | 适配新 API |

---

### Task 1: 重构 WideTableIncrementalUpdater 核心类

**Files:**
- Modify: `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java`

- [ ] **Step 1: 重写整个类**

替换整个文件内容为：

```java
package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

/**
 * 宽表增量更新器（重构版）
 * 
 * 核心功能：
 * 1. 四态判断：根据 before/after 主键集合判断 INSERT/UPDATE/DELETE/SKIP
 * 2. 可选事务：enableTransaction=true 时真实更新宽表，false 时仅生成事件
 * 3. Changelog 监听：通过 ChangelogListener 输出标准 TapdataEvent
 */
public class WideTableIncrementalUpdater {

    private static final Logger logger = LoggerFactory.getLogger(WideTableIncrementalUpdater.class);

    private final String wideTablePrimaryKey;
    private final String querySql;
    private final List<String> fields;
    private final WithCteSqlGenerator withCteSqlGenerator;
    private final DuckDbOperator duckDbOperator;
    private final FourStateJudge fourStateJudge;
    private final boolean enableTransaction;
    private final List<ChangelogListener> changelogListeners = new ArrayList<>();

    /**
     * Changelog 监听器接口
     */
    @FunctionalInterface
    public interface ChangelogListener {
        void onEvent(TapdataEvent event);
    }

    /**
     * 构造函数（非事务模式）
     */
    public WideTableIncrementalUpdater(String tableId, String wideTablePrimaryKey, String querySql,
                                       List<String> fields, WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator) {
        this(tableId, wideTablePrimaryKey, querySql, fields, withCteSqlGenerator, duckDbOperator, false);
    }

    /**
     * 构造函数（完整）
     * @param enableTransaction 是否启用事务模式（true=真实更新宽表，false=仅生成事件）
     */
    public WideTableIncrementalUpdater(String tableId, String wideTablePrimaryKey, String querySql,
                                       List<String> fields, WithCteSqlGenerator withCteSqlGenerator,
                                       DuckDbOperator duckDbOperator, boolean enableTransaction) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
        this.querySql = querySql;
        this.fields = fields;
        this.withCteSqlGenerator = withCteSqlGenerator;
        this.duckDbOperator = duckDbOperator;
        this.fourStateJudge = new FourStateJudge(tableId, wideTablePrimaryKey);
        this.enableTransaction = enableTransaction;
    }

    /**
     * 添加 Changelog 监听器
     */
    public void addChangelogListener(ChangelogListener listener) {
        if (listener != null) {
            changelogListeners.add(listener);
        }
    }

    /**
     * 批量更新宽表（唯一核心方法）
     * 
     * 事务模式：包裹 executeInTransaction + 真实更新宽表 + 生成事件
     * 非事务模式：仅生成事件，不更新宽表
     * 
     * @param affectedBeforeKeys before 受影响主键集合
     * @param affectedAfterKeys after 受影响主键集合
     * @param afterRows after 数据行（从 CDC 事件提取）
     * @param tableName 源表名（用于 WITH CTE 临时表名）
     * @return TapdataEvent 事件列表
     */
    public List<TapdataEvent> updateWideTableAsTapdataEvents(Set<Object> affectedBeforeKeys,
                                                              Set<Object> affectedAfterKeys,
                                                              List<Map<String, Object>> afterRows,
                                                              String tableName) throws SQLException {
        final List<TapdataEvent>[] resultHolder = new List[]{Collections.emptyList()};

        if (enableTransaction) {
            // 事务模式：真实更新宽表 + 生成事件
            duckDbOperator.executeInTransaction(() -> {
                resultHolder[0] = executeAndUpdate(affectedBeforeKeys, afterRows, tableName);
            });
        } else {
            // 非事务模式：仅生成事件，不更新宽表
            resultHolder[0] = executeAndUpdate(affectedBeforeKeys, afterRows, tableName);
        }

        return resultHolder[0];
    }

    /**
     * 执行查询 + 四态判断 + 可选宽表更新
     */
    private List<TapdataEvent> executeAndUpdate(Set<Object> affectedBeforeKeys,
                                                 List<Map<String, Object>> afterRows,
                                                 String tableName) throws SQLException {
        // 1. 使用 WITH CTE 执行 after 查询
        List<Map<String, Object>> results = Collections.emptyList();
        if (afterRows != null && !afterRows.isEmpty()) {
            String afterSql = withCteSqlGenerator.generateBatch(querySql, tableName, afterRows, fields);
            results = duckDbOperator.executeQuery(afterSql);
        }

        // 2. 四态判断
        List<TapdataEvent> events = fourStateJudge.judge(affectedBeforeKeys, results);

        // 3. 事务模式下真实更新宽表
        if (enableTransaction && !events.isEmpty()) {
            applyEventsToWideTable(events);
        }

        // 4. 触发 ChangelogListener
        for (TapdataEvent event : events) {
            for (ChangelogListener listener : changelogListeners) {
                try {
                    listener.onEvent(event);
                } catch (Exception e) {
                    logger.warn("Error notifying changelog listener", e);
                }
            }
        }

        return events;
    }

    /**
     * 将事件应用到宽表（真实执行 INSERT/UPDATE/DELETE）
     */
    private void applyEventsToWideTable(List<TapdataEvent> events) throws SQLException {
        // 按操作类型分组，批量执行
        List<Map<String, Object>> inserts = new ArrayList<>();
        List<Object> deletePks = new ArrayList<>();

        for (TapdataEvent event : events) {
            com.tapdata.entity.event.TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapInsertRecordEvent) {
                inserts.add(((TapInsertRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapUpdateRecordEvent) {
                // UPDATE = DELETE old + INSERT new
                Map<String, Object> before = ((TapUpdateRecordEvent) tapEvent).getBefore();
                if (before != null && before.containsKey(wideTablePrimaryKey)) {
                    deletePks.add(before.get(wideTablePrimaryKey));
                }
                inserts.add(((TapUpdateRecordEvent) tapEvent).getAfter());
            } else if (tapEvent instanceof TapDeleteRecordEvent) {
                Map<String, Object> before = ((TapDeleteRecordEvent) tapEvent).getBefore();
                if (before != null && before.containsKey(wideTablePrimaryKey)) {
                    deletePks.add(before.get(wideTablePrimaryKey));
                }
            }
        }

        // 批量删除
        for (Object pk : deletePks) {
            deleteRowByPk(pk);
        }

        // 批量插入
        if (!inserts.isEmpty()) {
            duckDbOperator.batchInsert("wide_table", inserts);
        }
    }

    /**
     * 按主键删除宽表记录
     */
    private void deleteRowByPk(Object pk) throws SQLException {
        String pkValue;
        if (pk instanceof String) {
            pkValue = "'" + pk.toString().replace("'", "''") + "'";
        } else {
            pkValue = pk.toString();
        }

        String deleteSql = String.format(
                "DELETE FROM wide_table WHERE %s = %s",
                wideTablePrimaryKey,
                pkValue
        );
        logger.debug("Deleting row: {}", deleteSql);
        duckDbOperator.executeUpdate(deleteSql);
    }
}
```

- [ ] **Step 2: 编译验证**

```bash
cd /Users/hj/workspace/tapdata && mvn compile -pl iengine/iengine-app -q
```

Expected: BUILD SUCCESS

- [ ] **Step 3: 提交**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdater.java
git commit -m "refactor: simplify WideTableIncrementalUpdater to single method with transaction support and ChangelogListener"
```

---

### Task 2: 重写 WideTableIncrementalUpdaterTest

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java`

- [ ] **Step 1: 重写整个测试类**

替换整个文件内容为：

```java
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
    void testUpdateWideTableAsTapdataEvents_deleteAndInsert() throws SQLException {
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
    void testUpdateWideTableAsTapdataEvents_primaryKeyUpdate() throws SQLException {
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
    void testUpdateWideTableAsTapdataEvents_onlyDelete() throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>(Arrays.asList(123, 456));
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, Collections.emptyList(), "users");

        assertEquals(2, result.size());
        assertEquals(2, countByType(result, TapDeleteRecordEvent.class));
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_onlyInsert() throws SQLException {
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
    void testUpdateWideTableAsTapdataEvents_emptyBoth() throws SQLException {
        Set<Object> affectedBeforeKeys = new LinkedHashSet<>();
        Set<Object> affectedAfterKeys = new LinkedHashSet<>();

        List<TapdataEvent> result = updater.updateWideTableAsTapdataEvents(
                affectedBeforeKeys, affectedAfterKeys, Collections.emptyList(), "users");

        assertTrue(result.isEmpty());
    }

    @Test
    void testUpdateWideTableAsTapdataEvents_noChange() throws SQLException {
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
    void testUpdateWideTableAsTapdataEvents_TransactionMode_CallsExecuteInTransaction() throws SQLException {
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
    void testUpdateWideTableAsTapdataEvents_TransactionMode_RollbackOnError() throws SQLException {
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
    void testUpdateWideTableAsTapdataEvents_NonTransactionMode_DoesNotCallExecuteInTransaction() throws SQLException {
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
    void testUpdateWideTableAsTapdataEvents_ChangelogListener_ReceivesEvents() throws SQLException {
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
    void testUpdateWideTableAsTapdataEvents_ChangelogListener_ExceptionDoesNotBreakFlow() throws SQLException {
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
        assertEquals(1, successCount.get());
        assertEquals(2, result.size());
    }

    // ==================== WITH CTE 测试 ====================

    @Test
    void testUpdateWideTableAsTapdataEvents_WithCteIntegration() throws SQLException {
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
```

- [ ] **Step 2: 运行测试验证**

```bash
cd /Users/hj/workspace/tapdata && mvn test -Dtest=WideTableIncrementalUpdaterTest -pl iengine/iengine-app -q 2>&1 | tail -5
```

Expected: All tests pass

- [ ] **Step 3: 提交**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WideTableIncrementalUpdaterTest.java
git commit -m "test: rewrite WideTableIncrementalUpdater tests for new API"
```

---

### Task 3: 更新集成测试适配新 API

**Files:**
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeIntegrationTest.java`
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteIntegrationTest.java`
- Modify: `iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/BatchWideTableUpdateIntegrationTest.java`

- [ ] **Step 1: 修改 FourStateJudgeIntegrationTest**

读取现有文件，将 `WideTableIncrementalUpdater` 构造函数调用改为新签名：

```java
// 旧:
WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
        "id", "users", fields, sqlGenerator, mockDuckDbOperator);

// 新:
WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
        "wide_table", "id",
        "SELECT id, name, email FROM users",
        fields, sqlGenerator, mockDuckDbOperator, false);
```

测试方法调用保持不变（已经是 `updateWideTableAsTapdataEvents`）。

- [ ] **Step 2: 修改 WithCteIntegrationTest**

将 `updateWideTable` 调用改为 `updateWideTableAsTapdataEvents`，更新构造函数：

```java
// 旧构造函数:
WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
        "id", "users", fields, sqlGenerator, mockDuckDbOperator);

// 新构造函数:
WideTableIncrementalUpdater updater = new WideTableIncrementalUpdater(
        "wide_table", "id",
        "SELECT id, name, email FROM users",
        fields, sqlGenerator, mockDuckDbOperator, false);

// 旧方法调用:
List<WideTableCdcEvent> events = updater.updateWideTable(
        affectedBeforeKeys, affectedAfterKeys, afterRows, "users");

// 新方法调用:
List<TapdataEvent> events = updater.updateWideTableAsTapdataEvents(
        affectedBeforeKeys, affectedAfterKeys, afterRows, "users");
```

更新断言：
```java
// 旧断言:
assertEquals(2, events.size());
assertEquals(WideTableCdcEvent.OpType.DELETE, events.get(0).getOpType());

// 新断言:
assertEquals(2, events.size());
assertTrue(events.get(0).getTapEvent() instanceof TapDeleteRecordEvent);
```

- [ ] **Step 3: 修改 BatchWideTableUpdateIntegrationTest**

同 WithCteIntegrationTest 的修改模式：
- 更新构造函数
- 更新方法调用
- 更新断言

- [ ] **Step 4: 运行所有集成测试**

```bash
cd /Users/hj/workspace/tapdata && mvn test -Dtest="FourStateJudgeIntegrationTest,WithCteIntegrationTest,BatchWideTableUpdateIntegrationTest" -pl iengine/iengine-app -q 2>&1 | tail -5
```

Expected: All tests pass

- [ ] **Step 5: 提交**

```bash
cd /Users/hj/workspace/tapdata
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/FourStateJudgeIntegrationTest.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/WithCteIntegrationTest.java
git add iengine/iengine-app/src/test/java/io/tapdata/flow/engine/V2/node/duckdb/BatchWideTableUpdateIntegrationTest.java
git commit -m "test: update integration tests for new WideTableIncrementalUpdater API"
```

---

### Task 4: 全量测试验证与收尾

- [ ] **Step 1: 运行所有相关测试**

```bash
cd /Users/hj/workspace/tapdata && mvn test -Dtest="WideTableIncrementalUpdaterTest,FourStateJudgeTest,FourStateJudgeIntegrationTest,WithCteIntegrationTest,BatchWideTableUpdateIntegrationTest,WithCteSqlGeneratorTest" -pl iengine/iengine-app --fail-at-end 2>&1 | grep -E "(Tests run|BUILD|FAILURE)"
```

Expected: All tests pass, BUILD SUCCESS

- [ ] **Step 2: 删除废弃的 WideTableCdcEvent（如果不再被引用）**

```bash
cd /Users/hj/workspace/tapdata && grep -r "WideTableCdcEvent" --include="*.java" iengine/iengine-app/src/main/
```

如果主代码无引用，标记为 `@Deprecated` 或保留（由用户决定）。

- [ ] **Step 3: 最终提交**

```bash
cd /Users/hj/workspace/tapdata
git status
```

---

## Self-Review

1. **Spec coverage:** 
   - 删除旧方法 ✓ (Task 1)
   - 单一核心方法 ✓ (Task 1)
   - 事务支持（可选）✓ (Task 1)
   - ChangelogListener ✓ (Task 1)
   - 事务模式真实更新宽表 ✓ (Task 1 applyEventsToWideTable)
   - 非事务模式不更新宽表 ✓ (Task 1)
   - 测试覆盖 ✓ (Task 2, 3)

2. **Placeholder scan:** 无 TBD/TODO

3. **Type consistency:** 
   - `TapdataEvent`、`TapInsertRecordEvent`、`TapUpdateRecordEvent`、`TapDeleteRecordEvent` 与 FourStateJudge 一致
   - `DuckDbOperator.ThrowingConsumer` 与接口定义一致
   - `ChangelogListener` 接口定义与 spec 一致
