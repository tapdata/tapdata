# D08 Recovery 批次超时扫描

状态：已完成  
完成日期：2026-08-28  
依赖：D03 任务锁、D06 回调状态机、D07 回调幂等

## 本步骤完成内容

### 1. 批次超时筛选与终态更新

`DqlRecoveryBatchRepository` 新增：

- `findTimedOut(deadline)`：只扫描 `DISPATCHED`、`RUNNING` 且 `updated <= deadline` 的批次，并按更新时间升序返回。
- `increaseFailed(batchId, count)`：一次性增加本轮超时事件数，刷新批次 `updated` 和 `ttl_at`。
- `finishTimedOut(batchId, status, message)`：只允许活动批次进入 `FAILED` 或 `PARTIAL_FAILED`，使用条件更新避免重复扫描覆盖终态。

默认批次超时时间为 1800 秒。批次 `updated` 会在派发、进入 RUNNING、事件结果计数和其他回调推进时刷新，因此没有回调进展的活动批次会被扫描。

### 2. 未完成事件超时补偿

`DqlEventRepository.timeoutEvents` 使用一条条件 `updateMulti` 处理当前批次事件：

- 条件同时限定 `current_batch_id=batchId`、`status=REPROCESSING` 和事件列表（列表为空时覆盖批次全部事件）。
- 追加 `TIMEOUT-{batchId}` 稳定 attempt，设置开始/结束时间和 `TIMEOUT` 结果。
- 事件转为 `RECOVERY_FAILED`，清空 `current_batch_id`，更新恢复摘要、`recovery_count` 和 TTL。
- 同一批次的重复扫描无法再次命中已补偿事件；迟到 Engine 回调也无法覆盖已释放的事件锁。

同时新增 `countReprocessingByBatchId`，用于在批次终态更新前确认没有永久遗留的当前批次事件。

### 3. 定时任务和锁释放

新增 `DqlRecoveryBatchTimeoutScheduler`：

- 每 30 秒触发，初始延迟 30 秒。
- 通过 `@SchedulerLock(name = "dql_recovery_batch_timeout", lockAtMostFor = "PT1M", lockAtLeastFor = "PT5S")` 避免多实例重复扫描。
- 委托 `DqlRecoveryBatchService.timeoutExpiredBatches`，每个批次先补偿事件，再确认事件全部收敛。
- 没有成功事件时进入 `FAILED`；已有成功事件并伴随超时事件时进入 `PARTIAL_FAILED`。
- 批次终态条件更新成功后触发一次恢复失败告警并释放 D03 任务锁；仍有事件未收敛时不释放锁，留待下一轮扫描。

## 代码与文档产出

- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlEventRepository.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlRecoveryBatchRepository.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/schedule/DqlRecoveryBatchTimeoutScheduler.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEventRepositoryTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlRecoveryBatchRepositoryTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/schedule/DqlRecoveryBatchTimeoutSchedulerTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/docs/superpowers/plans/2026-08-28-TAP-12615-D08.md`
- `/Users/gavinxiao/kit/tapdata/tapdata/docs/superpowers/specs/2026-08-28-TAP-12615-D08-recovery-batch-timeout-design.md`

## 验证结果

D08 定向回归命令：

```text
mvn -o -pl manager/tm -am -Djacoco.skip=true \
  -Dtest='DqlRecoveryBatchServiceTest,DqlEventRepositoryTest,DqlRecoveryBatchRepositoryTest,DqlRecoveryBatchTimeoutSchedulerTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
```

结果：

```text
DqlEventRepositoryTest              Tests run: 20, Failures: 0, Errors: 0
DqlRecoveryBatchRepositoryTest      Tests run: 14, Failures: 0, Errors: 0
DqlRecoveryBatchTimeoutSchedulerTest Tests run: 1, Failures: 0, Errors: 0
DqlRecoveryBatchServiceTest         Tests run: 35, Failures: 0, Errors: 0
总计                                Tests run: 70, Failures: 0, Errors: 0
BUILD SUCCESS
```

完整 DQL 回归（包含 DQL 定向测试及本步骤定时任务测试）结果：`Tests run: 124, Failures: 0, Errors: 0`，`BUILD SUCCESS`。

## 后续依赖

D09 在本步骤提供的批次状态、计数、完成时间和消息基础上完善批次详情与审计展示。D10 将把并发发起、重复回调、部分失败、批次失败和超时恢复纳入统一 TM 回归。
