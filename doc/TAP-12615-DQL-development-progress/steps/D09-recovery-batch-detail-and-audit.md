# D09 批次详情和审计

状态：已完成  
完成日期：2026-08-28  
依赖：D06 回调状态机、D07 回调幂等、D08 批次超时扫描

## 本步骤完成内容

### 1. 批次详情契约补齐

`DqlRecoveryBatchDto` 和 `DqlRecoveryBatchEntity` 新增并保持 snake_case 持久化映射：

- `mode`：当前固定支持 `AUTO`，创建时默认补齐，消息下发时从批次继承。
- `taskStatusBefore`、`taskStatusAfter`：记录任务业务状态前后值。当前 source gate 不改变任务业务状态，因此创建时将 after 固化为 before。
- `sourceReadPauseResult` / `sourceReadResumeResult` 及对应 message、时间字段：为运行中任务的源读取暂停/恢复提供可诊断结果。
- `auditEntries`：批次时间线，条目包含类型、状态、事件/attempt 标识、消息、发生时间和操作人。

批次详情读取会兼容 D09 之前的历史批次：缺失模式返回 `AUTO`，缺失任务后状态按前状态补齐，缺失审计数组返回空数组；这些兼容值只在响应映射中补齐，不回写历史文档。

### 2. 审计时间线

批次创建时写入 `BATCH_CREATED`，派发和合法回调推进时追加对应审计条目：

- `BATCH_DISPATCHED`、`BATCH_STARTED`。
- `EVENT_STARTED`、`EVENT_RESULT`，事件条目带 `eventId` 和 `attemptId`。
- `BATCH_FINISHED`、`BATCH_FAILED`、`BATCH_TIMEOUT`。

`DqlRecoveryBatchRepository.appendAudit` 使用条件更新并同步刷新 `updated` / `ttl_at`。重复回调只在 D07 判定为实际状态变化时追加，避免审计时间线和计数重复增长。

### 3. Source read gate 结果落库

`DqlRecoveryBatchService.recordSourceReadResult` 为后续 Engine source gate 提供内部记录入口。暂停或恢复结果与其 `auditEntries` 在同一次 Mongo 更新中写入，并使用同一时间刷新批次进度和 TTL；结果缺失或批次 ID 非法时按统一参数错误拒绝。

当前 D09 只提供 TM 侧契约、落库和诊断能力，不提前改变 Engine 的 source gate 行为；E04 将复用该入口记录实际暂停/恢复结果。

## 代码与文档产出

- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm-common/src/main/java/com/tapdata/tm/dql/dto/DqlRecoveryAuditEntryDto.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm-common/src/main/java/com/tapdata/tm/dql/dto/DqlRecoveryBatchDto.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm-common/src/main/java/com/tapdata/tm/dql/dto/DqlRecoveryMessageDto.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm-common/src/main/java/com/tapdata/tm/dql/vo/DqlRecoveryRequestVo.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/entity/DqlRecoveryBatchEntity.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/repository/DqlRecoveryBatchRepository.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm-common/src/test/java/com/tapdata/tm/dql/dto/DqlRecoveryMessageDtoTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlRecoveryBatchRepositoryTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEntityMappingTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/doc/TAP-12615-DLQ-controlled-reprocessing-detailed-design.md`
- `/Users/gavinxiao/kit/tapdata/tapdata/doc/TAP-12615-DLQ-controlled-reprocessing-api.md`
- `/Users/gavinxiao/kit/tapdata/tapdata/doc/TAP-12615-DLQ-controlled-reprocessing-development-plan.md`

## 验证结果

D09 定向回归命令：

```text
mvn -o -pl manager/tm -am -Djacoco.skip=true \
  -Dtest='DqlRecoveryMessageDtoTest,DqlRecoveryBatchRepositoryTest,DqlRecoveryBatchServiceTest,DqlEntityMappingTest,DqlEventControllerTest' \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DsurefireArgLine='-javaagent:/Users/gavinxiao/.m2/repository/org/mockito/mockito-core/5.20.0/mockito-core-5.20.0.jar' test
```

结果：`Tests run: 66, Failures: 0, Errors: 0`，`BUILD SUCCESS`。

完整 DQL 回归（包含 DQL 全部相关回调、Repository、Controller、实体映射和超时调度测试）结果：`Tests run: 128, Failures: 0, Errors: 0`，`BUILD SUCCESS`。

## 后续依赖

D10 汇总 TM 重处理的并发、锁冲突、下发失败、重复回调、部分失败、批次失败和超时补偿回归；E04 将把实际 source gate 的暂停/恢复结果接入 D09 已提供的批次审计入口。
