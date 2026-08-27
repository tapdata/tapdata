# E08 恢复事件防递归捕获

## 1. 步骤结论

E08 已完成。本步骤把恢复事件从现有 Engine DQL 捕获链路中隔离出来：恢复 DML 写入失败时，只通知当前回放事件的屏障和 attempt 结果，不再次调用普通 DQL 主记录上报；普通事件仍沿用原有捕获、skip 和后续成功回调逻辑。

## 2. 实际实现

### 2.1 公共失败桥接

- 在 `iengine-common` 增加 `DqlRecoveryFailureRegistry`，按 DQL `eventId` 保存当前恢复 attempt 的一次性失败监听器。
- 注册发生重复时直接拒绝；失败通知采用“先移除、后回调”，因此目标写入和处理节点的重复捕获不会重复完成同一个屏障。
- 监听器回调异常不会覆盖原始写入异常，原有任务错误路径仍可以继续处理。
- 在 `DqlRecoveryCaptureGuard` 集中识别 `DQL_RECOVERY`、`DQL_EVENT_ID` 等恢复标记，并提供从 `TapdataEvent`/`TapRecordEvent` 通知原始失败的统一入口。

### 2.2 与 E07 屏障连接

- `DqlRecoveryBarrierCoordinator.register(eventId)` 在恢复 DML 注入处理图之前注册失败监听器。
- 捕获链路通知失败后，监听器把对应事件置为 `FAILED` 并释放屏障；`await`、超时和注入失败都会清理本地 pending 状态及注册表。
- 协调器在事件加载、构造、注入或等待失败时取消该事件注册，避免失败路径遗留监听器。

### 2.3 目标和处理节点捕获边界

- 目标写入实际通过 `SkipErrorDataAspect` 进入 `SkipErrorEventAspectTask`，仓库中不存在计划初稿所写的 `HazelcastTargetPdkDataNode`。单条恢复记录失败时，`checkSkip` 先通知恢复屏障并返回“不跳过”，调用方因此重新抛出同一个原始异常。
- 处理节点 `HazelcastProcessorBaseNode.interceptProcessorError` 在执行普通 `SkipErrorProcessAspect` 之前识别恢复事件，通知失败桥并保留既有 `errorHandle` 路径。
- 后续成功回调跳过恢复记录，避免一次回放成功被误记为普通 DQL 事件的覆盖成功。
- 非恢复事件没有新的分支效果，仍按原来的分类、Storm Guard、DQL 上报和 skip 语义处理。

## 3. TDD 与验证记录

本步骤的专项测试覆盖：

- 恢复失败监听器只能消费一次，普通事件不会进入恢复失败路径；
- 目标写入失败保留精确的原始异常，且不与 `DqlEventReporter` 发生交互；
- 处理节点失败通知原 attempt，且不创建新的 DQL 主记录；
- E07 屏障、E03 协调器与恢复捕获测试共同验证失败结果可以唤醒对应事件。

专项命令：

```bash
mvn -o -pl iengine/modules/skip-error-event-module,iengine/iengine-app -am \
  -Dtest=DqlRecoveryCaptureGuardTest,SkipErrorEventRecoveryCaptureTest,DqlRecoveryBarrierCoordinatorTest,DqlRecoveryCoordinatorImplTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：`iengine-app` 相关测试 12 项、skip-error 模块恢复捕获测试 2 项全部通过；受影响 Reactor 编译通过。

随后执行 C12 及捕获回归：`C12EngineCaptureRegressionTest` 3/3 通过，E08 新增恢复捕获测试 2/2 通过。扩大执行既有 `SkipErrorEventAspectTaskTest` 时出现 3 个 `LogSkipEventTest` 断言失败和 5 个 `ExecutorShutdownTest` 异常：前者由测试数据 getter 为空导致日志数量变化，后者由 Mockito 伪造 `ThreadPoolExecutor` 的内部 `ctl` 为空导致 NPE；两类堆栈均位于未改动的既有测试夹具，未进入 E08 生产分支，因此作为基线测试问题保留，不把它们归因于本步骤。

另外执行 `git diff --check`，无空白错误。

## 4. 设计取舍与边界

- 失败桥放在 `iengine-common`，避免 skip-error 模块依赖 `iengine-app`，并让处理节点与目标捕获共享同一身份契约。
- 失败注册以 DQL `eventId` 为键；同一批次的不同 attempt 由 E07 屏障生命周期串行管理，避免并发回放误完成其他事件。
- 本步骤不改变 TM 回调协议；TM 侧的原事件状态和 attempt 收敛仍由 E07/E09 的结果回调与补偿逻辑负责。
- `reportEventStarted`、结果回调发送失败以及 runner/source gate 的跨资源清理属于 E09，不在 E08 扩大捕获守卫职责。

## 5. 代码产出

- `iengine/iengine-common/src/main/java/io/tapdata/dql/recovery/DqlRecoveryFailureRegistry.java`
- `iengine/iengine-common/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCaptureGuard.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryBarrierCoordinator.java`
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/processor/HazelcastProcessorBaseNode.java`
- `iengine/modules/skip-error-event-module/src/main/java/io/tapdata/task/skiperrorevent/SkipErrorEventAspectTask.java`
- `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryCaptureGuardTest.java`
- `iengine/modules/skip-error-event-module/src/test/java/io/tapdata/task/skiperrorevent/SkipErrorEventRecoveryCaptureTest.java`

## 6. 后续步骤

进入 E09，补齐批次级失败补偿：覆盖报告/启动回调失败、runner 初始化、source gate 恢复、任务停止或重启等路径，确保本地资源释放且 TM 最终可以通过回调或超时扫描收敛。
