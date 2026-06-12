# FourStateJudge 四态判断器设计文档

> **解耦四态判断逻辑，输出 Tapdata 标准 CDC 事件**

**版本**：V1.0  
**日期**：2026-05-26  
**状态**：已批准

---

## 1. 项目背景

### 1.1 当前状态

`WideTableIncrementalUpdater` 内置四态判断逻辑：
- 计算纯 DELETE 主键（before - after）
- 执行 SQL 获取 after 数据
- 判断 INSERT/UPDATE/DELETE
- 输出 `WideTableCdcEvent`（自定义事件类型）

### 1.2 问题

- 判断逻辑与 SQL 执行耦合，无法独立测试
- `WideTableCdcEvent` 是自定义类型，无法直接对接下游节点
- 缺少 SKIP 类型，无法表达"无变化"场景

### 1.3 目标

创建 `FourStateJudge` 独立组件：
- 解耦四态判断逻辑
- 输出 `TapdataEvent`（Tapdata 标准事件）
- 支持 INSERT/UPDATE/DELETE/SKIP 四态判断

---

## 2. 核心方案

### 2.1 架构

```
FourStateJudge
├── 输入：beforePks (Set<Object>), afterData (List<Map<String, Object>>)
├── 判断逻辑：四态判断（INSERT/UPDATE/DELETE/SKIP）
└── 输出：List<TapdataEvent>（标准 Tapdata 事件）
```

### 2.2 判断逻辑

| 条件 | 操作 | 说明 |
|------|------|------|
| 有旧无新 | DELETE | pk 在 beforePks 中但不在 afterData 中 |
| 无旧有新 | INSERT | pk 不在 beforePks 中但在 afterData 中 |
| 新旧都有 | UPDATE | pk 同时在 beforePks 和 afterData 中 |
| 无旧无新 | SKIP | 不输出事件 |

### 2.3 数据流

```
WideTableIncrementalUpdater
  → 执行 SQL 获取 beforePks 和 afterData
  → FourStateJudge.judge(beforePks, afterData)
  → 返回 List<TapdataEvent>
  → 输出到下游节点
```

---

## 3. 类设计

### 3.1 FourStateJudge

```java
public class FourStateJudge {
    private final String tableId;
    private final String wideTablePrimaryKey;
    
    public FourStateJudge(String tableId, String wideTablePrimaryKey);
    
    public List<TapdataEvent> judge(Set<Object> beforePks, List<Map<String, Object>> afterData);
}
```

### 3.2 与现有集成

1. **WideTableIncrementalUpdater** 依赖 FourStateJudge 进行判断
2. **WideTableCdcEvent** 标记为 `@Deprecated`，后续可移除
3. **测试**：FourStateJudge 独立单元测试 + 集成测试

---

## 4. 错误处理

- beforePks 或 afterData 为 null → 返回空列表
- afterData 中缺少 wideTablePrimaryKey → 跳过该行并记录警告

---

## 5. 测试覆盖

### 5.1 单元测试

- INSERT 场景（无旧有新）
- UPDATE 场景（新旧都有）
- DELETE 场景（有旧无新）
- SKIP 场景（无旧无新）
- 混合场景（INSERT + UPDATE + DELETE）
- 边界场景（null 输入、缺少主键）

### 5.2 集成测试

- FourStateJudge + WideTableIncrementalUpdater 联合工作
- 验证输出 TapdataEvent 格式正确

---

## 6. 实施计划

### Task 1: 创建 FourStateJudge 类
- 编写测试用例
- 实现 judge 方法
- 运行测试验证

### Task 2: 集成到 WideTableIncrementalUpdater
- 修改 WideTableIncrementalUpdater 使用 FourStateJudge
- 标记 WideTableCdcEvent 为 @Deprecated
- 运行集成测试验证

### Task 3: 清理旧代码
- 移除 WideTableCdcEvent（可选）
- 运行全部测试验证无回归
