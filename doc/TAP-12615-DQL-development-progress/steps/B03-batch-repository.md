# B03 批次 Repository

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B01

## 完成内容

- `dql_recovery_batches` 创建时补齐默认状态 `CREATED` 和四类计数 0，避免调用方遗漏字段造成批次统计为空。
- 保留批次 ID 唯一索引、任务/状态查询索引和 Mongo 原子更新实现。
- 批次状态推进按状态条件更新：`CREATED -> DISPATCHED`、`DISPATCHED -> RUNNING`；批次结束只允许活动状态进入终态，普通回调不能覆盖已结束批次。
- 成功、失败、跳过计数通过单次 `$inc` 更新，并仅作用于 `CREATED`、`DISPATCHED`、`RUNNING` 批次；同次刷新 `updated` 和 `ttl_at`。
- 新增按任务查询最新活动批次能力，筛选 `CREATED`、`DISPATCHED`、`RUNNING` 并按创建时间倒序返回，为后续 D03 同任务互斥控制提供 Repository 基础。

## 验证

- Repository 单元测试覆盖：创建默认值、TTL 初始化、状态条件、活动批次筛选/排序、计数原子更新和终态更新边界。
- Maven 定向测试受当前 Codex 授权服务 503 阻断，未能在本次执行中启动；代码已修复此前测试缺少 `java.util.List` 导入的编译问题。

## 后续依赖

- D03 使用 `findActiveByTaskId` 配合同任务批次锁，保证并发发起只有一个活动批次。
- D06/D07 需要继续在 Service 层处理回调状态机和重复回调幂等；Repository 的状态条件只作为底层保护。
