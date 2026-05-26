# WideTableIncrementalUpdater 重构设计

## 目标

清理 WideTableIncrementalUpdater 旧方法，统一为单一核心方法，新增事务支持（可选）和 Changelog 监听器。

## 架构

### 类结构

```
WideTableIncrementalUpdater
├── 构造函数参数
│   ├── tableId: String（宽表标识）
│   ├── wideTablePrimaryKey: String（宽表主键字段）
│   ├── querySql: String（宽表查询 SQL）
│   ├── fields: List<String>（字段列表）
│   ├── withCteSqlGenerator: WithCteSqlGenerator
│   ├── duckDbOperator: DuckDbOperator
│   └── enableTransaction: boolean（可选，默认 false）
├── ChangelogListener 接口
│   └── void onEvent(TapdataEvent event)
└── updateWideTableAsTapdataEvents 方法
    ├── 1. 使用 WITH CTE 执行 after 查询
    ├── 2. FourStateJudge 四态判断
    ├── 3. 如果 enableTransaction=true，包裹在 executeInTransaction 中 + 真实更新宽表
    └── 4. 通过 ChangelogListener 输出事件
```

### 事务模式行为差异

| 模式 | 宽表更新 | 事件生成 | 用途 |
|------|----------|----------|------|
| **事务模式** (`enableTransaction=true`) | 执行真实宽表 INSERT/UPDATE/DELETE | 生成 TapdataEvent + ChangelogListener 回调 | 生产环境，数据一致性 |
| **非事务模式** (`enableTransaction=false`) | 不更新宽表 | 仅生成 TapdataEvent + ChangelogListener 回调 | 测试/预演，只读判断 |

### 数据流

```
CDC Events → AffectedKeyCalculator → before/after keys
                                        ↓
WideTableIncrementalUpdater.updateWideTableAsTapdataEvents
    ├── WITH CTE 查询 → 获取最新宽表数据
    ├── FourStateJudge.judge → INSERT/UPDATE/DELETE/SKIP
    ├── [事务模式] applyEventsToWideTable → 真实执行 INSERT/UPDATE/DELETE
    └── ChangelogListener.onEvent → 输出标准 TapdataEvent
```

## 组件设计

### ChangelogListener 接口

```java
@FunctionalInterface
public interface ChangelogListener {
    void onEvent(TapdataEvent event);
}
```

### 核心方法签名

```java
public List<TapdataEvent> updateWideTableAsTapdataEvents(
    Set<Object> affectedBeforeKeys,
    Set<Object> affectedAfterKeys,
    List<Map<String, Object>> afterRows,
    String tableName
) throws SQLException
```

### 宽表更新方法

```java
private void applyEventsToWideTable(List<TapdataEvent> events) throws SQLException {
    for (TapdataEvent event : events) {
        TapEvent tapEvent = event.getTapEvent();
        if (tapEvent instanceof TapInsertRecordEvent) {
            duckDbOperator.batchInsert(wideTableName, Collections.singletonList(((TapInsertRecordEvent) tapEvent).getAfter()));
        } else if (tapEvent instanceof TapUpdateRecordEvent) {
            // UPDATE = DELETE old + INSERT new
            TapUpdateRecordEvent updateEvent = (TapUpdateRecordEvent) tapEvent;
            deleteRowByPk(updateEvent.getBefore().get(wideTablePrimaryKey));
            duckDbOperator.batchInsert(wideTableName, Collections.singletonList(updateEvent.getAfter()));
        } else if (tapEvent instanceof TapDeleteRecordEvent) {
            deleteRowByPk(((TapDeleteRecordEvent) tapEvent).getBefore().get(wideTablePrimaryKey));
        }
    }
}
```

## 错误处理

- 事务模式：`executeInTransaction` 自动回滚异常
- 非事务模式：异常向上抛出
- ChangelogListener 异常：捕获并记录日志，不影响主流程

## 删除的旧方法

- `updateWideTable(Set<Object>, Set<Object>)` - 旧版 WideTableCdcEvent 方法
- `updateWideTable(Set<Object>, Set<Object>, List<Map>, String)` - 旧版 WITH CTE 方法
- `generateAfterSql(Set<Object>)` - 旧版 SQL 生成
- `buildInClause(Set<Object>)` - 旧版 IN 子句构建
- `formatValue(Object)` - 旧版值格式化

## 测试策略

- 修改现有测试用例使用新方法
- 新增事务回滚测试
- 新增 ChangelogListener 回调测试
- 新增事务模式真实更新宽表测试
- 新增非事务模式不更新宽表测试
- 保持 FourStateJudgeIntegrationTest 不变
