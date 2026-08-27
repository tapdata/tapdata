# TAP-12615 D08 Recovery 批次超时设计

## 范围

D08 负责 TM 对回调丢失、Engine 异常退出和派发后无进展的批次进行定时补偿，不实现 Engine recovery handler、单事件屏障或批次详情 API。默认批次超时时间为 1800 秒，扫描周期为 30 秒。

## 批次扫描

```text
deadline = now - 1800s
find status in [DISPATCHED, RUNNING] and updated <= deadline
sort updated ASC
```

Repository 返回批次快照，Service 对每个批次执行事件补偿和终态收敛。`finishTimedOut` 必须带活动状态条件，防止回调或其他扫描器已经完成批次后被覆盖。

## 事件超时更新

条件：

```text
current_batch_id = batchId
status = REPROCESSING
event_id in batchEventIds       # 为空时使用批次全部事件
recovery_attempts 中不存在 batchId + TIMEOUT-{batchId}
```

更新：

```text
status = RECOVERY_FAILED
current_batch_id = null
last_recovery_result = TIMEOUT
recovery_count += 1
append recovery_attempt(result=TIMEOUT, attemptId=TIMEOUT-{batchId})
updated = ttl_at = now
```

同一批次的 synthetic timeout attempt 使用稳定身份，允许扫描任务安全重试。事件更新和 Engine 结果回调都要求当前批次的 `REPROCESSING` 锁，因此先完成的路径不会被另一条路径覆盖。

## 批次终态与副作用顺序

```text
for timedOutBatch:
  timeoutEvents(batchId)
  increaseFailed(timedOutCount)
  if countReprocessingByBatchId(batchId) > 0:
    continue
  latest = findByBatchId(batchId)
  status = successCount > 0 ? PARTIAL_FAILED : FAILED
  if finishTimedOut(batchId, status):
    alarm.notifyRecoveryFailed(batch)
    taskLock.release(taskId, batchId)
```

只有 `finishTimedOut` 原子应用成功时才触发告警和释放锁。这样重复扫描、多个 TM 实例和批次终态迟到回调不会重复产生外部副作用。

## 调度

`DqlRecoveryBatchTimeoutScheduler` 使用：

```java
@Scheduled(fixedDelay = 30_000L, initialDelay = 30_000L)
@SchedulerLock(
    name = "dql_recovery_batch_timeout",
    lockAtMostFor = "PT1M",
    lockAtLeastFor = "PT5S")
```

调度器保持薄层，只负责触发 Service；查询、补偿和状态收敛可在单元测试中独立验证。

## 测试要求

- Batch Repository：状态/截止时间筛选、活动批次终态条件更新、多事件失败计数。
- Event Repository：TIMEOUT attempt 字段、事件锁释放、恢复次数和 TTL、批次归属计数。
- Batch Service：全失败、部分失败、仍有遗留 `REPROCESSING`、告警和任务锁释放。
- Scheduler：调度入口委托 Service。
