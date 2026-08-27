# D02 固化回放顺序

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：D01
- 范围：TM 统一排序规则、预览顺序、批次 `ordered_event_ids` 和 `dqlRecovery` 消息顺序一致性
- 不包含：D03 同任务并发互斥锁、D04 事件锁事务补偿和 Engine 回放执行器

## 完成内容

### 1. 统一服务端排序规则

新增 `DqlRecoveryOrder`，将回放顺序集中定义为：

```text
task_id ASC,
event_time ASC,
capture_seq ASC,
event_id ASC
```

其中任务 ID 用于跨任务排序确定性；D01 已限制一次重处理只能选择同一个任务。`event_time` 和 `capture_seq` 为空时排在非空值之后，`event_id` 作为最终稳定 tie-breaker。

预览先过滤不可重处理事件，再调用统一排序组件生成 `orderedEvents`，不再在服务方法内维护另一份排序实现。

### 2. 固化批次和下发顺序

- `start` 只从预览返回的 `orderedEvents` 生成 `orderedEventIds`。
- 批次创建时将该列表写入 `ordered_event_ids`，同时保留用户原始选择在 `event_ids` 中。
- `dispatch` 只读取批次的 `orderedEventIds` 组装 `dqlRecovery` 消息；Mongo 查询返回顺序不会改变实际下发顺序。
- Engine 后续必须按消息中的 `orderedEventIds` 串行执行，不在 Engine 端重新排序。

### 3. 回归覆盖

- 覆盖多任务排序键、同一事件时间、同一捕获序号和事件 ID tie-breaker。
- 覆盖多表事件从预览到批次和消息的顺序保持一致。
- 覆盖批次 `orderedEventIds` 与消息 `eventIds` 均使用服务端固化顺序。

## TDD 与验证结果

### RED

先新增排序规则测试，既有代码不存在 `DqlRecoveryOrder`，测试编译按预期失败：

```text
找不到符号：变量 DqlRecoveryOrder
```

补充服务集成后，发现 `DqlRecoveryOrder.sort` 返回 `List` 而服务仍按 `Stream` 调用 `.map`，按编译错误修正为显式 `.stream()`，未修改测试来规避问题。

### GREEN

定向执行 D02 测试：

```text
mvn -o -pl manager/tm -am \
  -Djacoco.skip=true \
  -Dtest='DqlRecoveryOrderTest,DqlRecoveryBatchServiceTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
Tests run: 17, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

全量 DQL/TM 相关回归共 99 个测试，全部通过；覆盖 DQL 事件、恢复批次、Controller、Mapper、实体映射、TTL patch 和既有回调测试。仓库已有 Maven model、Lombok、过时 API、Logback 和 Java agent 相关警告未作为本步骤错误处理。

## 设计边界与后续依赖

- D02 只固化确定性顺序，不提供并发互斥；D03 负责同任务活动批次的原子锁和恢复策略。
- D04 继续负责事件锁定与批次创建事务语义；D05 负责消息契约 DTO 化和下发失败补偿。
- 本步骤创建独立本地 commit，未执行 push。提交后暂停开发，等待用户 code review 和运行验证确认，不进入 D03。
