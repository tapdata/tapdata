# TAP-12615 D03 同任务重处理锁设计

## 目标

为 TM 重处理批次增加任务级互斥，保证同一任务在 CREATED、DISPATCHED、RUNNING 任一活动状态期间只能存在一个重处理批次；TM 进程异常退出后，过期租约可以被下一次提交安全回收。

## 现状与边界

- D01 的 preview 会查询活动批次，但该查询是普通读操作，不能解决两个并发 start 同时通过预览的问题。
- D02 已固化批次顺序；D03 不改变 eventIds、orderedEventIds 或消息顺序。
- D04 负责事件级条件锁定和批次创建事务补偿；D08 负责批次超时扫描、事件超时 attempt 和显式清理。
- D03 只实现任务锁的原子抢占、租约回收和批次生命周期释放，不提前实现 Engine 回放或定时超时扫描。

## 方案

使用独立 Mongo 集合 dql_recovery_locks，而不是复用通用 LockServiceImpl：

    db.dql_recovery_locks.createIndex(
      { task_id: 1 },
      { unique: true, name: "uk_task_id" }
    )
    db.dql_recovery_locks.createIndex(
      { expire_at: 1 },
      { name: "idx_expire_at" }
    )

锁文档字段：

    {
      "task_id": "64f...",
      "batch_id": "DQLB-20260827-000001",
      "owner": "tm",
      "expire_at": 1787581800000,
      "created": 1787580000000
    }

batch_id 是本次持有者 token，owner 保留部署来源信息。唯一 task_id 索引保证同一任务只能有一条锁记录。

### 原子抢锁

DqlRecoveryTaskLockRepository.tryAcquire(taskId, batchId) 使用带租约条件的 findAndModify：

1. 只匹配同一 task_id 且 expire_at <= now 或没有 expire_at 的记录，匹配到时原子更新当前 batch_id、owner、expire_at。
2. 没有锁记录时使用 upsert 尝试插入；并发插入触发唯一索引 DuplicateKeyException，竞争请求转换为抢锁失败。整个抢锁只使用一次 findAndModify，不在应用层做先查后插。
3. 未过期的已有锁不匹配，返回 false，服务抛出 DqlRecovery.BatchAlreadyRunning。

默认租约为 dql.recovery.batchTimeoutSeconds=1800 对应的 30 分钟，D03 通过常量沿用该契约值。后续配置接入时只替换租约来源，不改变仓储原子语义。

### 生命周期

    preview（提示性活动批次检查）
        -> tryAcquire(taskId, batchId)
           -> 失败：BatchAlreadyRunning，不创建批次、不锁事件、不下发
           -> 成功：创建 CREATED 批次
              -> 创建/事件锁/下发失败：补偿并释放任务锁
              -> 下发成功：保持任务锁
                 -> BATCH_STARTED / EVENT_RESULT：保持任务锁
                 -> BATCH_FINISHED / BATCH_FAILED：先记录终态，再释放任务锁

终态释放按 task_id + batch_id 条件执行，避免旧批次释放已经被过期回收并重新分配给新批次的锁。释放操作幂等。

进程异常退出或回调丢失时不依赖内存状态；锁在 expire_at 到期后可被后续提交原子回收。D08 的超时扫描仍需负责批次和事件状态收敛。

## 服务兼容

- Spring 生产构造注入任务锁仓储。
- 保留现有测试和兼容调用使用的旧构造器；未提供任务锁仓储时不启用 D03 锁，避免改变既有单元测试夹具的行为。
- 生产 start 以任务锁结果作为并发最终裁决，不能只依赖 findActiveByTaskId。
- 新增 BatchAlreadyRunning 的默认、英文、简体中文和繁体中文消息，并同步 API 错误语义文档。

## 验收标准

- 同一任务的两个并发提交最多一个可以创建批次并进入下发流程。
- 未过期锁冲突不产生批次、事件锁或恢复消息。
- 创建、事件锁定、消息下发失败均释放任务锁。
- 正常终态和失败终态均释放任务锁；重复释放不影响其他批次。
- 过期锁可以被下一次任务批次原子回收。
- 仓储索引、服务生命周期、错误码和既有 DQL TM 回归测试通过。
