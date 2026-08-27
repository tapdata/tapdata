# E09 批次失败补偿

## 1. 步骤结论

E09 已完成。恢复批次现在有统一的失败补偿边界：无论是 runner 初始化、source gate 准备/恢复、事件回调还是批次终态回调失败，都会尽可能发送一次 `BATCH_FAILED`，并执行已注册资源的逆序清理。清理异常和 TM 回调异常不会覆盖导致批次失败的原始异常。

## 2. 实际实现

### 2.1 `DqlRecoveryFailureCompensator`

新增 `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryFailureCompensator.java`：

- 资源按创建顺序注册，使用 LIFO 顺序清理，保证 source gate、runner、执行器等生命周期按逆序释放。
- 清理和失败报告分别使用原子状态控制；重复补偿不会重复释放资源或重复发送 `BATCH_FAILED`。
- 清理动作逐个执行，即使前一个资源关闭失败，后续资源仍然获得关闭机会；清理失败只作为诊断，不从补偿入口抛出。
- 失败报告采用 best effort；TM 不可用时不阻断本地清理，也不改变原始异常的消息。
- 补偿已经开始后才注册的资源会立即获得一次关闭尝试，避免 late registration 留下资源。

### 2.2 Coordinator 生命周期

`DqlRecoveryCoordinatorImpl` 现在：

- paused-task runner 创建成功后立即注册关闭动作；runner 初始化失败直接进入批次补偿。
- live source boundary 支持 `prepareForRecovery(timeoutMillis)` 和 `restoreAfterRecovery()` 钩子。`DqlSourceBoundaryInjector` 将钩子转发到实际 DAG source boundary，实际 source adapter 可以在钩子中操作 E04 `DqlSourceReadGate`。
- 在 source 准备前先注册恢复动作，因此准备过程部分成功后抛错也会执行恢复；成功批次在发送 `BATCH_FINISHED` 前先完成资源清理。
- `reportEventStarted` 已纳入事件异常边界；事件已经注册屏障但启动报告失败时会取消该事件屏障，随后由批次补偿收敛。
- `reportEventResult` 和 `reportBatchFinished` 回调失败会进入同一补偿路径。终态成功报告发送失败时释放终态标记，再尽可能发送 `BATCH_FAILED`，避免错误地把批次留在“已完成但 TM 未收到”的本地状态。
- 单事件失败且策略为停止时也走补偿器，不再绕过 runner/source gate 清理。

### 2.3 Handler 接收边界

`DqlRecoveryMessageHandler` 保留 E01 的幂等策略：

- Coordinator 尚未接受前失败会释放 batch claim，允许 TM 重试初始化。
- Coordinator 已接受但 `BATCH_STARTED` 报告失败时保留 claim，避免重复消息重新回放；同时 best effort 发送一次 `BATCH_FAILED`，后续由 TM 回调幂等或超时扫描收敛。
- Engine 进程被直接终止或重启无法执行进程内清理，继续由 TM 的批次超时扫描、事件锁释放和批次补偿承担最终收敛。

## 3. 测试与验证

新增及扩展测试覆盖：

- 三项补偿器测试：逆序清理、清理/报告失败隔离、重复补偿和 late resource registration；
- 两项 Coordinator 测试：paused runner 初始化失败，以及 live source gate 恢复失败；
- source gate 生命周期恢复测试；
- Handler 的 `BATCH_STARTED` 回调失败后保留 claim 并发送 `BATCH_FAILED`。

Focused 命令：

```bash
mvn -o -pl iengine/iengine-app -am \
  -Dtest=DqlRecoveryFailureCompensatorTest,DqlRecoveryCoordinatorImplTest,DqlRecoveryMessageHandlerTest,DqlSourceReadGateTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：`iengine-app` 相关测试 20 项全部通过，Reactor 编译成功；包括 E09 新增补偿器 3 项、Coordinator 7 项、Handler 7 项和 source gate 3 项。

另行尝试既有 `DataSyncEventHandlerTest` stop/reset/delete 消息回归时，两个嵌套测试在进入生产逻辑前因 Mockito `SubclassByteBuddyMockMaker` 不支持 static mock 而失败。该测试需要 `mockito-inline`，属于当前工程测试运行配置的既有限制，本步骤没有修改依赖或测试夹具。

执行 `git diff --check`，无空白错误。

## 4. 代码产出

- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryFailureCompensator.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlReplaySourceNode.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlSourceBoundaryInjector.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlSourceReadGate.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryMessageHandler.java`
- `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryFailureCompensatorTest.java`
- 相关 Coordinator、Handler 和 source gate 回归测试

## 5. 后续步骤

进入 E10，建立 Engine 回放回归矩阵，串联 E01-E09 的 live/paused、I/U/D、顺序、屏障、失败策略、超时、重复消息、重启和 source gate 恢复证据，并收口 M4。
