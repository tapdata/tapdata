# D05 `dqlRecovery` 消息下发

状态：已完成  
完成日期：2026-08-27  
依赖：D04 事件级锁和批次创建补偿

## 本步骤完成内容

### 1. 独立消息 DTO

在 `tm-common` 新增 `DqlRecoveryMessageDto`，固定消息类型 `dqlRecovery`，包含：

- `taskId`
- `batchId`
- `taskVersion`
- D02 固化的 `orderedEventIds`
- `operatorId`、`operatorName`
- `mode=AUTO`

DTO 提供从 `DqlRecoveryBatchDto` 构造消息和转换为现有 pipe 传输所需 Map 的能力。消息不包含 `opType`，也不再使用未排序的 `eventIds`，避免与 `DataSyncMq` 任务生命周期语义混用。

### 2. TM 下发顺序

`DqlRecoveryBatchService` 的批次启动顺序调整为：

1. 创建 `CREATED` 批次。
2. 按 D04 条件锁定全部事件。
3. 将批次更新为 `DISPATCHED`。
4. 将完整 `DqlRecoveryMessageDto` 发送到批次记录的 `agentId`。

派发失败仍由 D04 补偿：按事件原始状态解锁、批次置 `FAILED`、释放任务租约。Engine 接受消息和回调不在本步骤提前实现，分别由 E01、D06-D07 负责。

## 代码产出

- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm-common/src/main/java/com/tapdata/tm/dql/dto/DqlRecoveryMessageDto.java`
- `/Users/gavinxiao/kit/tapdata/tapdata/manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- `DqlRecoveryMessageDtoTest` 与 `DqlRecoveryBatchServiceTest` 消息契约/发送顺序测试

## 验证结果

TDD 记录：先新增消息契约测试，因 DTO 尚不存在而在 `tm-common` 测试编译阶段失败；实现 DTO 后修复一个既有服务测试仍读取旧 `eventIds` 的断言，并验证完整消息。

定向测试结果：

```text
DqlRecoveryMessageDtoTest     Tests run: 1, Failures: 0, Errors: 0
DqlRecoveryBatchServiceTest   Tests run: 21, Failures: 0, Errors: 0
BUILD SUCCESS
```

## 后续依赖

D06 需要使用本步骤的 `batchId/taskId/orderedEventIds/taskVersion` 校验 Engine 结果回调；E01 需要按 `type=dqlRecovery` 注册独立 handler，并根据 `mode=AUTO` 选择 live 或 recovery-only 执行模式。
