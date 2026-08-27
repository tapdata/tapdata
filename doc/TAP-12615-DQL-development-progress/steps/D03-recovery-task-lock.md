# D03 同任务重处理批次互斥锁

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B03、D01、D02
- 范围：TM 任务级重处理锁、锁租约回收、批次生命周期释放和锁冲突错误语义
- 不包含：D04 事件级锁与批次创建事务语义、D05 消息 DTO 化、D06/D07 回调状态机与幂等、D08 超时扫描

## 完成内容

### 1. 独立锁集合和索引

新增 `dql_recovery_locks` 集合对应的 `DqlRecoveryTaskLockEntity` 与 `DqlRecoveryTaskLockRepository`，锁文档包含：

- `task_id`：任务唯一互斥键；
- `batch_id`：当前批次持有者 token；
- `owner`：固定为 `tm`；
- `expire_at`：租约到期时间；
- `created`：首次创建时间。

仓储初始化以下索引：

- `uk_task_id`：`task_id` 唯一索引，保证一个任务最多一条锁记录；
- `idx_expire_at`：过期时间普通索引，为后续过期扫描和诊断提供查询入口。

### 2. 原子抢锁和租约回收

- `tryAcquire` 使用一次带条件的 Mongo `findAndModify(upsert=true, returnNew=true)`，只匹配同一任务且锁不存在或 `expire_at <= now` 的记录。
- 抢锁成功时原子写入当前 `batch_id`、`owner` 和新的 `expire_at`；默认租约为 1800 秒，与 `dql.recovery.batchTimeoutSeconds` 当前契约一致。
- 并发创建空锁记录触发唯一索引 `DuplicateKeyException` 时转换为抢锁失败，不向调用方泄露数据库异常。
- `release` 同时匹配 `task_id + batch_id`，旧批次不能删除过期回收后属于新批次的锁；重复释放是安全的。

### 3. 接入批次生命周期

生产构造器注入任务锁仓储，`start` 在批次创建前生成 `batch_id` 并抢锁：

```text
preview（活动批次检查仅用于提示）
  -> tryAcquire(task_id, batch_id)
     -> 冲突：BatchAlreadyRunning，不创建批次、不锁事件、不下发
     -> 成功：创建 CREATED 批次
        -> 创建/事件锁/下发失败：补偿并释放任务锁
        -> 下发成功：保持任务锁覆盖 DISPATCHED/RUNNING
           -> BATCH_FINISHED/BATCH_FAILED：记录终态后释放任务锁
```

预览接口继续展示已有活动批次，提交接口不把这次普通读作为最终裁决，而以原子任务锁为准。这样旧批次记录仍在但任务锁租约已过期时，新的提交可以安全回收租约并继续后续流程。原有五参数和七参数构造器保留，用于兼容既有测试夹具；Spring 生产构造器启用 D03 锁。

新增错误码 `DqlRecovery.BatchAlreadyRunning`，同步默认、英文、简体中文、繁体中文消息以及 API/详细设计的 HTTP 409 错误表。

## 测试与验证

### TDD RED

- 任务锁仓储测试先于实现创建，初次执行因实体和仓储不存在而测试编译失败。
- 服务生命周期测试先于八参数生产构造器和锁接入创建，初次执行因构造器及 `BatchAlreadyRunning` 行为不存在而测试编译失败。
- 消息资源测试先于四种 locale 文案创建，执行后稳定失败并返回消息 key `DqlRecovery.BatchAlreadyRunning`，随后补齐资源。
- 增加“活动批次记录已陈旧但任务锁可回收”的提交回归测试，约束预览提示与提交原子裁决分离。

### GREEN

定向验证结果：

```text
DqlRecoveryTaskLockRepositoryTest: Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
DqlRecoveryBatchServiceTest: Tests run: 19, Failures: 0, Errors: 0, Skipped: 0
```

最终提交前已执行 DQL TM 相关仓储、服务、Controller、错误消息和实体映射回归，共 97 项全部通过；`git diff --check` 通过。Maven model、Lombok、过时 API、Logback 和 Java agent 相关警告属于仓库既有告警，不作为本步骤失败依据。

## 设计边界与后续依赖

- D03 只保证任务级活动批次互斥和租约回收，不负责把事件全部原子锁为 `REPROCESSING`；该事务补偿由 D04 完成。
- D03 不实现批次超时状态收敛。进程异常退出后任务锁可在租约到期后重新获取，但旧批次和事件的状态、TIMEOUT attempt 仍由 D08 扫描器处理。
- D03 不改变 D02 固化的 `ordered_event_ids` 和 `dqlRecovery` 消息顺序。
- 本步骤完成后创建独立本地 commit，不执行 push；按当前用户授权自动进入 D04 分析。
