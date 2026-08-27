# E03 `DqlRecoveryCoordinator`

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-28
- 依赖：E01、E02
- 范围：Engine 单批次异步串行调度、事件/批次 recovery 回调和失败继续策略

## 分析结论

E01 的 WebSocket Handler 负责消息校验、批次幂等 claim 和 `BATCH_STARTED`；E02 已提供从安全快照重建 DML 的 `TapdataDqlRecoveryEvent`。因此 E03 只承接执行编排，不把 Mongo 查询、Jet source 节点或目标完成判断写死在 Coordinator 中。

本步骤定义四个端口：事件源返回 `DqlPayloadSnapshot`，事件 sink 将 recovery event 注入源边界，barrier 返回 `SUCCESS/FAILED/TIMEOUT`，execution policy 决定单事件失败后继续或停止。这样 E04-E07 可以分别接入运行中 source gate、暂停任务 runner、DAG source boundary 和真实逐事件 latch，而不会改变 E03 的批次顺序状态机。

TM 当前 recovery API 支持五类报告，但 Engine 旧适配器只发送 `BATCH_STARTED`。本步骤将 `DqlRecoveryReportSender` 扩展为统一发送器，并补齐 `EVENT_STARTED`、`EVENT_RESULT`、`BATCH_FINISHED` 和 `BATCH_FAILED` 报告构造；旧的 Handler 调用保持为默认便捷方法。

## 完成内容

- 新增 `DqlRecoveryCoordinatorImpl`，`start` 只负责校验并提交异步任务，不阻塞 WebSocket 线程。
- 对每个批次复制 `orderedEventIds`，使用进程内 active map 拒绝同批次并发执行，并用原子终态标记保证只发送一次批次完成/失败报告。
- 每条事件按固定顺序执行：创建 `attemptId` → `EVENT_STARTED` → 加载完整快照 → 创建 E02 recovery event → sink 注入 → barrier 等待 → `EVENT_RESULT`。
- barrier 失败或超时映射为对应事件终态；策略允许时继续下一条，否则发送 `BATCH_FAILED`；全部完成后发送 `BATCH_FINISHED`。
- barrier 中断时恢复线程中断标记并以 `TIMEOUT` 结果结束当前事件，避免吞掉取消信号。
- `DqlRecoveryReportSender` 现在由 `DqlRecoveryEventHandler` 统一转发到已有 `DqlEventReporter.reportRecovery`，保持 TM 回调入口不变。

## 代码产出

- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryEventSource.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryEventSink.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryBarrier.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryExecutionPolicy.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryReportSender.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/model/DqlRecoveryReport.java`
- `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImplTest.java`

## TDD 与验证结果

### RED

先添加 Coordinator 测试并用包含 `manager/tm-common`、`iengine-common`、`iengine-app` 的离线 reactor 执行。测试编译按预期失败，失败点是四个端口与 `DqlRecoveryCoordinatorImpl` 尚不存在；直接只编译 `iengine-app` 还会因本地缓存未安装当前 Engine/TM 模块而误报既有 DQL 类型缺失，因此后续统一使用 reactor 命令。

### GREEN

执行：

```text
mvn -o -pl manager/tm-common,iengine/iengine-common,iengine/iengine-app -am -Dtest=DqlRecoveryCoordinatorImplTest,DqlRecoveryMessageHandlerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：`DqlRecoveryCoordinatorImplTest` 3 项、`DqlRecoveryMessageHandlerTest` 7 项，共 10 项测试通过，0 失败，0 错误，`BUILD SUCCESS`。reactor 中 TM Common、IENGINE Common、IENGINE APP 及其必要模块均成功编译。Maven 仍输出仓库已有的重复依赖、过时 API 和 annotation processor 警告，本步骤未引入新的编译错误。

## 后续依赖

- E04 用 `DqlRecoveryEventSink` 接入 live source read gate，并在普通事件与 recovery event 之间建立排空边界。
- E05 用同一 Coordinator 选择暂停任务 recovery-only runner，保证不启动普通 source reader。
- E07 将 `DqlRecoveryBarrier` 替换为真实 `TapdataCountDownLatchEvent` 及目标完成回调。
- E09 需要在报告失败、任务停止和执行器异常时补齐当前 Coordinator 的资源补偿。
