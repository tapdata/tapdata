# B03 批次 Repository

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B01

## 完成内容

- `dql_recovery_batches` 创建时补齐默认状态 `CREATED` 和四类计数 0，避免调用方遗漏字段造成批次统计为空。
- 保留批次 ID 唯一索引、任务/状态查询索引和 Mongo 原子更新实现。
- 批次状态推进按状态条件更新：通用 `updateStatus` 只接受 `CREATED -> DISPATCHED`，`markRunning` 只接受 `DISPATCHED -> RUNNING`。
- 批次结束只接受终态，并按 A02 限制来源：`SUCCESS` 仅来自 `RUNNING`，`PARTIAL_FAILED` 来自 `DISPATCHED`/`RUNNING`，`FAILED` 来自任一活动状态，`CANCELED` 仅来自 `CREATED`；普通回调不能覆盖已结束批次。
- 成功、失败、跳过计数通过单次 `$inc` 更新，并仅作用于 `CREATED`、`DISPATCHED`、`RUNNING` 批次；同次刷新 `updated` 和 `ttl_at`。
- 新增按任务查询最新活动批次能力，筛选 `CREATED`、`DISPATCHED`、`RUNNING` 并按创建时间倒序返回，为后续 D03 同任务互斥控制提供 Repository 基础。

## 验证

- Repository 单元测试覆盖：创建默认值、TTL 初始化、状态条件、活动批次筛选/排序、计数原子更新、非法目标状态拒绝和终态来源约束。
- 2026-08-27 A01-B03 回归已实际执行本组 10 个测试并通过，消除了原步骤记录中 Maven 未能启动的验证缺口。

## 后续依赖

- D03 使用 `findActiveByTaskId` 配合同任务批次锁，保证并发发起只有一个活动批次。
- D06/D07 需要继续在 Service 层处理回调状态机和重复回调幂等；Repository 的状态条件只作为底层保护。
