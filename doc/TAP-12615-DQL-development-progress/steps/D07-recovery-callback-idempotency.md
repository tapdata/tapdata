# D07 Recovery 回调幂等

状态：已完成  
完成日期：2026-08-28  
依赖：D06 Engine 结果回调状态机

## 本步骤完成内容

### 1. 事件回调按 attempt 原子去重

新增 `DqlRecoveryCallbackResultEnum`，用 `APPLIED`、`DUPLICATE`、`NOT_IN_BATCH` 和 `CONFLICT` 表达一次条件更新的结果。

`DqlEventRepository` 新增幂等事件迁移方法：

- `startEventIdempotent` 以 `eventId + batchId + attemptId` 为边界，只允许当前批次的 `REPROCESSING` 事件追加一次 `RUNNING` attempt。
- `completeEventIdempotent` 和 `failEventIdempotent` 在事件仍属于当前批次且不存在相同 attempt 时，原子追加终态 attempt、更新事件状态和摘要、增加 `recovery_count` 并刷新 TTL。
- 条件更新未命中时读取事件和 attempt 历史：同一批次、事件和 attempt 已达到相同结果返回 `DUPLICATE`；同一 attempt 却要求不同终态返回 `CONFLICT`；事件不属于当前批次返回 `NOT_IN_BATCH`。
- D06 已有的 boolean 事件迁移方法保留，以兼容原有调用方；回调 Service 使用新增幂等方法。

### 2. Service 只对首次应用的结果计数

`DqlRecoveryBatchService` 根据 Repository 结果处理事件回调：

- 只有 `APPLIED` 才增加批次 success/failed/skipped 计数；重复回调不增加计数，不重复告警，不追加 attempt。
- `CONFLICT` 返回 `DqlRecovery.AttemptConflict`；`NOT_IN_BATCH` 返回 `DqlRecovery.EventNotInBatch`。
- `BATCH_STARTED` 重复收到 RUNNING 回调、`BATCH_FINISHED` 重复收到 SUCCESS/PARTIAL_FAILED 回调、`BATCH_FAILED` 重复收到 FAILED 回调均直接幂等返回。
- 终态批次中，能够匹配已保存 attempt 的重复事件回调直接返回；未匹配的回调仍受终态状态保护。

## 代码与文档产出

- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm-common/src/main/java/com/tapdata/tm/dql/DqlRecoveryCallbackResultEnum.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlEventRepository.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEventRepositoryTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlEventServiceTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/docs/superpowers/plans/2026-08-28-TAP-12615-D07.md`
- `/Users/gavinxiao/kit/tapdata/tapdata/docs/superpowers/specs/2026-08-28-TAP-12615-D07-recovery-callback-idempotency-design.md`

## 验证结果

定向回归命令：

```text
mvn -o -pl manager/tm -am -Djacoco.skip=true \
  -Dtest='DqlRecoveryBatchServiceTest,DqlEventServiceTest,DqlEventRepositoryTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
```

结果：

```text
DqlEventRepositoryTest       Tests run: 18, Failures: 0, Errors: 0
DqlEventServiceTest          Tests run: 34, Failures: 0, Errors: 0
DqlRecoveryBatchServiceTest  Tests run: 32, Failures: 0, Errors: 0
总计                         Tests run: 84, Failures: 0, Errors: 0
BUILD SUCCESS
```

完整 DQL 回归命令：

```text
mvn -o -pl manager/tm -am -Djacoco.skip=true \
  -Dtest='DqlRecoveryMessageDtoTest,DqlRecoveryTaskLockRepositoryTest,DqlRecoveryBatchServiceTest,DqlEventServiceTest,DqlRecoveryBatchRepositoryTest,DqlEventRepositoryTest,DqlEventControllerTest,DqlErrorSemanticsTest,DqlEntityMappingTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
```

结果：`Tests run: 115, Failures: 0, Errors: 0`，`BUILD SUCCESS`。

## 后续依赖

D08 负责扫描 `DISPATCHED/RUNNING` 超时批次，为仍处于 `REPROCESSING` 的事件追加 `TIMEOUT` attempt，并在超时收敛后释放任务锁。D07 的 attempt 判重结果将作为超时补偿避免与迟到回调重复写入。
