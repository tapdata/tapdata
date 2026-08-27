# D10 TM 重处理全量回归

状态：已完成  
完成日期：2026-08-28  
依赖：D01-D09

## 本步骤完成内容

### 1. 并发和锁冲突

补充并发发起回归：两个请求同时对同一任务发起重处理时，只有一个请求取得 D03 任务锁并继续创建、锁定和派发批次，另一个请求返回 `DqlRecovery.BatchAlreadyRunning`，不会创建批次或锁定事件。

已有 `DqlRecoveryTaskLockRepositoryTest` 继续覆盖活跃租约阻止第二次获取、过期租约原子回收、Mongo 唯一键竞争转换为锁冲突，以及释放操作必须匹配任务和批次所有者。

### 2. 批次生命周期和补偿

统一回归覆盖 D10 要求的路径：

- 事件锁数量不一致、事件锁异常和消息下发异常都会补偿事件状态，收敛批次并释放任务锁。
- 重复 `EVENT_STARTED`、重复 `EVENT_RESULT`、重复 `BATCH_STARTED`、重复 `BATCH_FINISHED` 和重复 `BATCH_FAILED` 不重复追加 attempt、计数、告警或锁副作用。
- `SUCCESS`、`SKIPPED`、`FAILED` 和 `PARTIAL_FAILED` 的事件计数语义分别正确；批次完成前强制满足 `selected=success+failed+skipped`。
- `BATCH_FAILED` 释放当前批次事件锁并触发失败告警；部分失败批次触发部分失败告警并释放任务锁。
- 超时扫描补偿未完成事件，重复扫描不重复追加超时 attempt；事件仍未收敛时保持批次和任务锁，全部收敛后才进入 `FAILED/PARTIAL_FAILED` 并释放锁。

### 3. 回归边界

D10 只负责 TM 侧批次控制和补偿测试。Engine 的 `dqlRecovery` 消息处理、恢复事件、协调器、source gate 和暂停任务 runner 分别由 E01-E09 开发；D09 已提供 source gate 结果的 TM 审计入口。

## 代码与文档产出

- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlRecoveryTaskLockRepositoryTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlRecoveryBatchRepositoryTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEventRepositoryTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/schedule/DqlRecoveryBatchTimeoutSchedulerTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/docs/superpowers/plans/2026-08-28-TAP-12615-D10.md`

## 验证结果

D10 定向回归命令：

```text
mvn -o -pl manager/tm -am -Djacoco.skip=true \
  -Dtest='DqlRecoveryBatchServiceTest,DqlRecoveryTaskLockRepositoryTest,DqlRecoveryBatchRepositoryTest,DqlEventRepositoryTest,DqlRecoveryBatchTimeoutSchedulerTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
```

结果：`Tests run: 81, Failures: 0, Errors: 0`，`BUILD SUCCESS`。

包含 D09 详情/审计测试在内的完整 DQL 回归结果：`Tests run: 130, Failures: 0, Errors: 0`，`BUILD SUCCESS`。

## 阶段 D 结论

D01-D10 已满足 TM 批次控制计划的测试完成标准：事件计数保持可对账，合法终态均释放任务锁，重复回调和超时补偿可重入。阶段 D 代码已提交本地，下一步自动进入 E01，开发 Engine `dqlRecovery` 消息 Handler。
