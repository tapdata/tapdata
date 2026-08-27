# D06 Engine 结果回调状态机

状态：已完成  
完成日期：2026-08-27  
依赖：A02 状态机契约、D05 `dqlRecovery` 消息下发

## 本步骤完成内容

### 1. 批次回调状态推进

`DqlRecoveryBatchService.report` 现在按批次当前状态处理全部五种回调：

- `BATCH_STARTED` 只允许 `DISPATCHED -> RUNNING`。
- `EVENT_STARTED` 和 `EVENT_RESULT` 只允许批次处于 `RUNNING`。
- `BATCH_FINISHED` 只允许 `RUNNING` 批次进入 `SUCCESS` 或 `PARTIAL_FAILED`。
- `BATCH_FAILED` 允许 `CREATED`、`DISPATCHED`、`RUNNING` 批次进入 `FAILED`，并释放当前批次事件锁和任务锁。
- 已进入终态的批次不会被回调继续推进，返回 `DqlRecovery.InvalidBatchState`。

批次状态仍由 `DqlRecoveryBatchRepository` 的条件更新保护，Service 在处理回调前再次校验读取到的状态，减少非法回调对批次生命周期的影响。

### 2. 事件启动和结果回调

新增 `DqlEventRepository.startEvent`：

- 查询条件同时限定 `event_id`、`status=REPROCESSING` 和 `current_batch_id=batchId`。
- 只追加 `RUNNING` recovery attempt，刷新 `updated` 和 `ttl_at`。
- 不改变事件状态、不增加 `recovery_count`，由后续终态结果完成事件迁移。

`EVENT_RESULT` 严格解析 `SUCCESS`、`FAILED`、`SKIPPED`、`TIMEOUT`，拒绝缺失、非法或 `RUNNING` 结果。成功结果调用 `completeEvent`，其他终态调用 `failEvent`；条件更新未命中时返回 `DqlRecovery.EventNotInBatch`，不会更新批次计数或发送失败告警。

事件归属校验优先使用 D02 固化的 `orderedEventIds`，并兼容历史批次的 `eventIds` 字段；`eventId` 和 `attemptId` 均为回调必填定位字段。

### 3. 批次完成对账

`BATCH_FINISHED` 在写入终态前强制校验：

```text
selected_count = success_count + failed_count + skipped_count
```

计数缺失、负数或不一致时返回 `DqlRecovery.CountMismatch`，不写批次终态，也不释放任务锁。对账通过后，全部成功进入 `SUCCESS`，存在失败或跳过进入 `PARTIAL_FAILED`，并保留既有部分失败告警和任务锁释放逻辑。

## 代码产出

- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlEventRepository.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEventRepositoryTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`

## 验证结果

定向回归命令：

```text
mvn -o -pl manager/tm -am -Djacoco.skip=true \
  -Dtest='DqlRecoveryBatchServiceTest,DqlEventRepositoryTest,DqlRecoveryBatchRepositoryTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
```

结果：

```text
DqlEventRepositoryTest          Tests run: 15, Failures: 0, Errors: 0
DqlRecoveryBatchRepositoryTest   Tests run: 11, Failures: 0, Errors: 0
DqlRecoveryBatchServiceTest      Tests run: 26, Failures: 0, Errors: 0
总计                             Tests run: 52, Failures: 0, Errors: 0
BUILD SUCCESS
```

## 设计边界与后续依赖

本步骤完成合法状态迁移、事件归属保护、attempt 记录和批次计数对账。相同 `attemptId`、事件和批次的重复回调如何做到不重复追加 attempt、不重复增加事件/批次计数，由 D07 继续实现；本步骤不提前引入重复回调幂等语义。
