# AffectedKeyCalculatorTest 重构设计文档

## 1. 背景与目标

### 1.1 现状问题
- 原测试文件 `AffectedKeyCalculatorTest.java` 共 2146 行，71 个测试场景
- 旧模式（`calculateAffectedKeys`）和新模式（`calculateAffectedBeforeKeys/AfterKeys`）测试混合
- 场景分类混乱，难以维护和扩展
- 两种模式覆盖不一致，部分场景只有旧模式测试

### 1.2 重构目标
- 采用场景化测试类分离（方案A）
- 每个场景类覆盖两种模式（旧模式 + 新模式）
- 所有71个现有测试场景完整迁移
- 提取公共基类，减少重复代码

## 2. 架构设计

### 2.1 目录结构
```
src/test/java/io/tapdata/flow/engine/V2/node/duckdb/
├── AffectedKeyCalculatorTestBase.java           # 抽象基类
└── scenarios/
    ├── MainTableScenariosTest.java              # 主表操作场景
    ├── FromTableScenariosTest.java              # 子表JOIN场景
    ├── EdgeCasesScenariosTest.java              # 边界与异常场景
    ├── BatchBoundaryScenariosTest.java          # 批量边界场景
    ├── ABAProblemScenariosTest.java             # ABA问题场景
    ├── JoinKeyUpdateScenariosTest.java          # JOIN KEY更新场景
    └── HelperMethodsTest.java                   # 辅助方法验证
```

### 2.2 基类设计

```java
abstract class AffectedKeyCalculatorTestBase {
    // 公共Mock
    protected DuckDbOperator mockDuckDbOperator;
    
    // 计算器工厂方法
    protected AffectedKeyCalculator createOldModeCalculator(fromTables, customQueries);
    protected AffectedKeyCalculator createNewModeCalculator(fromTables);
    
    // 事件构建器（旧模式格式）
    protected Map<String, Object> createInsertEvent(pkField, pkValue);
    protected Map<String, Object> createUpdateEvent(pkField, pkValue);
    protected Map<String, Object> createDeleteEvent(pkField, pkValue);
    protected Map<String, Object> createEventWithAfter(pkField, pkValue);
    protected Map<String, Object> createEventWithBefore(pkField, pkValue);
    
    // 事件构建器（新模式SmartMerger格式）
    protected List<Map<String, Object>> createSmartMergerInsertEvents(pkField, pkValues);
    protected List<Map<String, Object>> createSmartMergerUpdateEvents(pkField, oldPk, newPk);
    protected List<Map<String, Object>> createSmartMergerDeleteEvents(pkField, pkValues);
    protected List<Map<String, Object>> createSmartMergerMixedEvents(...);
    
    // 公共断言
    protected void assertContainsKeys(Set<Object> result, Object... expectedKeys);
    protected void assertEmptyKeys(Set<Object> result);
}
```

## 3. 场景分类与迁移映射

### 3.1 MainTableScenariosTest（主表操作场景）

| 序号 | 旧模式测试 | 新模式对应测试 |
|------|-----------|---------------|
| 1 | MainTableInsert | insert → beforeKeys空, afterKeys返回PK |
| 2 | MainTableUpdate | update → beforeKeys返回PK, afterKeys返回PK |
| 3 | MainTableDelete | delete → beforeKeys返回PK, afterKeys空 |
| 4 | WithAfterField | after字段提取 → afterKeys返回PK |
| 5 | WithBeforeField | before字段提取 → beforeKeys返回PK |
| 6 | PrimaryKeyInteger | Integer主键验证 |
| 7 | PrimaryKeyString | String主键验证 |

### 3.2 FromTableScenariosTest（子表JOIN场景）

| 序号 | 旧模式测试 | 新模式对应测试 |
|------|-----------|---------------|
| 1 | FromTableWithCustomQuery | WITH CTE单表查询 |
| 2 | MultipleFromTables | WITH CTE多表查询 |
| 3 | NonPrimaryKeyJoin | 非主键JOIN的WITH CTE |
| 4 | MultiTableChainedJoin | 链式JOIN的WITH CTE |
| 5 | MultipleJoinKeys_SingleQuery | 多JOIN键单查询 |
| 6 | MultipleJoinKeys_DifferentConfigs | 多JOIN键不同配置 |
| 7 | FromTableWithEmptyResult | WITH CTE空结果 |
| 8 | FromTableQueryFails | WITH CTE查询异常 |
| 9 | DuplicateJoinResults | WITH CTE结果去重 |
| 10 | PartialNullResults | WITH CTE部分null结果 |
| 11 | MixedPrimaryKeyTypes | 混合主键类型WITH CTE |
| 12 | NullCustomQueries | 无配置查询异常 |

### 3.3 EdgeCasesScenariosTest（边界与异常场景）

| 序号 | 旧模式测试 | 新模式对应测试 |
|------|-----------|---------------|
| 1 | NullEvents | null事件 → 空集合 |
| 2 | EmptyEvents | 空集合 → 空集合 |
| 3 | UnknownTable | 未知表 → 空集合 |
| 4 | MissingPrimaryKey | 缺少主键 → 空集合 |
| 5 | NullPrimaryKeyInData | 主键为null → 跳过 |
| 6 | NullFromTables | null fromTables → 正常处理 |
| 7 | NullAndEmptyValues | null/空字符串处理 |
| 8 | SqlSpecialCharacters | SQL特殊字符注入防护 |
| 9 | CaseInsensitiveTableName | 大小写不敏感表名 |
| 10 | MultipleAffectedKeys | 多事件去重 |

### 3.4 BatchBoundaryScenariosTest（批量边界场景）

| 序号 | 旧模式测试 | 新模式对应测试 |
|------|-----------|---------------|
| 1 | BatchBoundary_999 | 999条事件批量WITH CTE |
| 2 | BatchBoundary_1000 | 1000条事件批量WITH CTE |
| 3 | BatchBoundary_1001 | 1001条事件分批WITH CTE |

### 3.5 ABAProblemScenariosTest（ABA问题场景）

| 序号 | 旧模式测试 | 新模式对应测试 |
|------|-----------|---------------|
| 1 | ABA_1_ContinuousDuplicate | SmartMerger连续重复合并 |
| 2 | ABA_2_IntervalDuplicate | SmartMerger间隔重复合并 |
| 3 | ABA_3_CrossBatchDuplicate | SmartMerger跨批次合并 |
| 4 | ABA_4_ReverseOrderDuplicate | SmartMerger逆序合并 |
| 5 | ABA_5_InsertDeleteInsert | SmartMerger INSERT-DELETE-INSERT |
| 6 | ABA_6_InsertUpdateDelete | SmartMerger INSERT-UPDATE-DELETE |
| 7 | ABA_7_UpdateDeleteUpdate | SmartMerger UPDATE-DELETE-UPDATE |
| 8 | ABA_8_DeleteInsertDelete | SmartMerger DELETE-INSERT-DELETE |
| 9 | ABA_9_InsertUpdateUpdateDelete | SmartMerger多UPDATE合并 |
| 10 | ABA_10_JoinKeyUnchanged_PKDuplicate | JOIN键不变PK重复 |
| 11 | ABA_11_JoinKeyChanges_DifferentPKs | JOIN键变化不同PK |
| 12 | ABA_12_JoinKeyFromNullToValue | JOIN键从null到值 |
| 13 | ABA_13_JoinKeyAlternating | JOIN键交替变化 |
| 14 | ABA_14_3Dimensional_Scenario1 | 3维场景1 |
| 15 | ABA_15_3Dimensional_Scenario2 | 3维场景2 |

### 3.6 JoinKeyUpdateScenariosTest（JOIN KEY更新场景）

| 序号 | 旧模式测试 | 新模式对应测试 |
|------|-----------|---------------|
| 1 | ABA_10_JoinKeyUnchanged_PKDuplicate | JOIN KEY不变场景 |
| 2 | ABA_11_JoinKeyChanges_DifferentPKs | JOIN KEY变化场景 |
| 3 | - | testCalculateAffectedBeforeKeys_joinKeyMultipleUpdates |
| 4 | - | testCalculateAffectedAfterKeys_joinKeyMultipleUpdates |

### 3.7 HelperMethodsTest（辅助方法验证）

| 序号 | 旧模式测试 | 说明 |
|------|-----------|------|
| 1 | ExtractBeforePrimaryKey_fromBeforeField | 从before字段提取 |
| 2 | ExtractBeforePrimaryKey_fromTopLevel | 从顶层提取 |
| 3 | ExtractBeforePrimaryKey_fromMongoO2 | 从Mongo o2提取 |
| 4 | ExtractBeforePrimaryKey_fromMongoO | 从Mongo o提取 |
| 5 | ExtractBeforePrimaryKey_empty | 空值提取 |
| 6 | ExtractAfterPrimaryKey_fromAfterField | 从after字段提取 |
| 7 | ExtractAfterPrimaryKey_fromTopLevel | 从顶层提取 |
| 8 | ExtractAfterPrimaryKey_empty | 空值提取 |
| 9 | IsPrimaryKeyUpdated_true | 主键变更检测-是 |
| 10 | IsPrimaryKeyUpdated_false_sameKey | 主键变更检测-否(相同) |
| 11 | IsPrimaryKeyUpdated_false_insertOnly | 主键变更检测-否(仅插入) |
| 12 | IsPrimaryKeyUpdated_false_deleteOnly | 主键变更检测-否(仅删除) |

## 4. 测试策略

### 4.1 两种模式对比验证
每个场景的两种模式使用相同的事件数据，验证：
- 旧模式：`calculateAffectedKeys(tableName, events)` 返回的PK集合
- 新模式：`calculateAffectedBeforeKeys(eventsByTable)` + `calculateAffectedAfterKeys(eventsByTable)` 返回的PK集合
- 验证逻辑：旧模式结果 = beforeKeys ∪ afterKeys（去重后）

### 4.2 Mock策略
- **旧模式**：Mock `executeQuery` 返回自定义JOIN查询结果
- **新模式**：Mock `executeQuery` 返回WITH CTE查询结果
- 使用 `anyString()` 匹配器，不区分SQL内容

### 4.3 SmartMerger事件格式
新模式测试使用统一的事件格式：
```java
// INSERT事件
Map.of("op", "INSERT", "id", 123)

// UPDATE事件（主键变更）
Map.of("op", "UPDATE", "o2", Map.of("id", 123), "updatedFields", Map.of("id", 456))

// UPDATE事件（主键不变）
Map.of("op", "UPDATE", "o2", Map.of("id", 123), "updatedFields", Map.of("name", "updated"))

// DELETE事件
Map.of("op", "DELETE", "o", Map.of("id", 123), "o2", Map.of("id", 123))
```

## 5. 实施步骤

1. 创建 `AffectedKeyCalculatorTestBase` 抽象基类
2. 创建 `scenarios/` 目录
3. 按场景分类逐个创建测试类，迁移现有测试
4. 为每个旧模式测试添加对应的新模式测试
5. 运行所有测试验证通过
6. 删除原 `AffectedKeyCalculatorTest.java` 文件

## 6. 验收标准

- 所有71个现有测试场景完整迁移
- 每个场景都有旧模式和新模式的对应测试
- 所有测试通过（BUILD SUCCESS）
- 测试文件总行数控制在合理范围（单文件不超过500行）
- 代码无重复，公共逻辑提取到基类
