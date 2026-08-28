# DQL 重处理心跳与超时兜底实施计划

## 目标

为 DQL 重处理批次建立 Engine 到 TM 的心跳机制，并由 TM 对未被 Engine 接管、Engine 执行线程未启动、执行中失联等情况进行及时兜底收敛，避免重处理记录长期停留在进行中。

实现只修改 Engine、TM、共享 DQL 回调协议和重处理批次元数据，不修改 MySQL、MongoDB 或其他业务数据源中的业务数据。

## 现状与约束

- TM 当前通过 `DqlRecoveryBatchTimeoutScheduler` 扫描 `DISPATCHED`、`RUNNING` 批次，但只依据 `updated`，默认批次超时为 1800 秒。
- Engine `DqlRecoveryCoordinatorImpl` 当前没有周期性重处理心跳。
- `DqlRecoveryBatchService` 已有事件超时补偿、批次失败收敛、任务锁释放和失败告警逻辑，应复用这些能力。
- Engine 的内存 `activeBatches` 不能被 TM 查询，持久化 `ping_time` 是 TM 判断 Engine 是否实际接管的依据。
- 历史批次可能没有 `ping_time`，必须继续使用原有 `updated` 规则兼容。

## 设计

### 1. 共享回调协议与批次字段

- 在 `DqlRecoveryReportTypeEnum` 和 Engine `DqlRecoveryReport` 中增加 `BATCH_HEARTBEAT`。
- 在回调模型中增加心跳时间戳 `pingTime`，使用 epoch milliseconds 传输。
- 在 `DqlRecoveryBatchDto`、`DqlRecoveryBatchEntity` 中增加 `ping_time` 字段及映射常量。
- 新增 `DqlRecoveryBatchRepository.touchHeartbeat(batchId, pingTime)`，只条件更新 `RUNNING` 批次的 `ping_time`、`updated`、`ttl_at`，终态批次的迟到心跳必须是 no-op。
- `markRunning` 在收到 `BATCH_STARTED` 时初始化 `ping_time`。

### 2. Engine 心跳生命周期

- Engine 恢复执行线程真正进入 `executeBatch` 后立即发送一次心跳。
- 使用协调器内部的共享定时执行器按配置间隔发送心跳，默认每 5 秒一次。
- 心跳只针对仍存在于 `activeBatches` 的批次发送。
- 批次完成、失败、线程中断或清理结束时取消该批次心跳。
- 心跳网络失败只记录日志，不阻断 DQL 重处理主流程；TM 会通过心跳失效兜底。
- 通过可注入的心跳调度依赖保证单元测试可控；生产调度器使用 daemon 线程，不阻塞 Engine 进程退出。

### 3. TM 分阶段超时扫描

继续使用带 ShedLock 的现有扫描任务，但让 Repository/Service 按状态使用不同截止时间：

- `DISPATCHED`：使用派发后的 `updated` 判断是否超过 `dispatchTimeoutSeconds`，默认 60 秒。未收到 `BATCH_STARTED` 时补偿事件并以失败结束，消息明确说明“重处理未被引擎接管”。
- `RUNNING`：优先使用 `ping_time` 判断是否超过 `heartbeatTimeoutSeconds`，默认 60 秒。心跳过期时补偿未完成事件并结束批次，消息明确说明“引擎重处理心跳超时”。
- 旧数据 `ping_time` 为空时，回退到原 `updated + batchTimeoutSeconds` 规则，不改变历史批次行为。
- 保持当前条件更新、迟到回调幂等、事件 `TIMEOUT-{batchId}` 记录、失败告警和任务锁释放逻辑。

### 4. 配置

在 `DqlRuntimeConfig` 增加并校验：

- `dql.recovery.dispatchTimeoutSeconds`，默认 60。
- `dql.recovery.heartbeatIntervalSeconds`，默认 5。
- `dql.recovery.heartbeatTimeoutSeconds`，默认 60。

保留现有 `dql.recovery.batchTimeoutSeconds=1800`，作为旧批次兼容和整体兜底。

## TDD 验证顺序

先新增并运行失败测试，确认测试确实覆盖缺失行为，再实现：

1. 共享模型和配置默认值、非法值回退。
2. Engine 首次心跳、周期心跳、终态停止和心跳发送失败不阻断执行。
3. TM 心跳只更新活动 `RUNNING` 批次，终态批次不被复活。
4. TM 对未接管 `DISPATCHED` 批次超时收敛。
5. TM 对心跳过期 `RUNNING` 批次超时收敛。
6. `ping_time` 为空的旧批次走兼容路径。
7. 现有 DQL 回调、事件补偿、调度器和旧数据测试回归。

## 风险与处理

- 心跳属于控制面写入，必须与事件业务写入解耦；心跳失败不能导致源端或目标端数据变更。
- 批次超时与 Engine 终态回调可能并发，所有终态和心跳更新必须使用状态条件，确保只由先成功的操作收敛。
- Engine 与 TM 版本可能短暂不一致；新增回调类型需要 TM/Engine 共享模型兼容，旧 Engine 没有心跳时由旧字段回退逻辑兜底。
- 心跳不能写入逐条审计记录，避免高频增长批次文档；详情只返回最新 `ping_time`。

## 完成标准

- 缺少 handler 或消息未到达 Engine 时，约一个调度超时周期内 TM 将批次标记失败并生成超时重处理记录。
- Engine 执行中失联时，约一个心跳超时周期内 TM 将批次标记失败并释放锁。
- 正常长时间重处理持续更新心跳，不被误判超时。
- 所有定向测试和相关模块编译通过，且工作区没有源端/目标端数据修改操作。
