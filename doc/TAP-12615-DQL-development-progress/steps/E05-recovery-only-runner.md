# E05：暂停任务 recovery-only runner

## 结论

E05 已完成。暂停任务不复用 `HazelcastTaskService.startTask`：该入口会创建 Jet Job，而正常 Source 节点在初始化时会启动 `initAndStartSourceRunner`，会违反暂停任务不启动普通读取的约束。

本步骤新增 recovery-only runner，以不可变 `TaskSnapshot` 表示任务 ID、版本和暂停状态，以仅暴露 recovery 入队能力的 `DqlReplaySourceNode` 作为源边界。runner 不创建 TaskClient、不启动 Jet Job、不调用连接器读取 API，只按调用顺序转发 `TapdataDqlRecoveryEvent` DATA 事件，并在关闭时按资源创建逆序释放。

## 代码路径

- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlReplaySourceNode.java`
  - 仅暴露 `enqueue(TapdataDqlRecoveryEvent)` 和 `close()`。
  - 接口没有普通 Source reader 或 connector start 能力，避免暂停回放误用正常启动路径。
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryOnlyRunner.java`
  - 校验任务快照为 `stop`、`paused` 或 `stopped` 状态。
  - 只接受 E02 DATA recovery event，拒绝 BEGIN/END 或普通事件。
  - `replay` 顺序调用 recovery source boundary，不改变快照状态。
  - `normalSourceStarted()` 固定返回 `false`，表达并固化 runner 的生命周期语义。
  - `close()` 幂等执行，先关闭 replay source，再逆序关闭 runner 资源，并确保所有资源都有关闭机会。
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
  - 增加可选 `DqlRecoveryOnlyRunner.Factory`。
  - Factory 返回 runner 时，批次事件通过 runner 注入；返回 `null` 时保持 live 任务原有 `DqlRecoveryEventSink` 路径。
  - 协调器统一负责批次结束后的 runner close，保留 E03 的单批次、按序、逐事件屏障行为。

## TDD 证据

### 红测与根因

首轮 testCompile 已通过新增类型；随后测试运行阶段稳定复现失败，完整堆栈指向 `TapdataDqlRecoveryEvent.createData`，原因是测试使用 `null` Payload。该错误来自 E02 的完整快照校验，而非 runner 生命周期；按系统化调试流程，使用现有 coordinator 测试中的 serializer 模式生成合法 `DqlPayloadSnapshot`，未放宽生产校验。

### 绿测

执行：

```bash
mvn -o -pl manager/tm-common,iengine/iengine-common,iengine/iengine-app -am \
  -Dtest=DqlRecoveryOnlyRunnerTest,DqlRecoveryCoordinatorImplTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：5 个测试通过，0 failures，0 errors；TM Common、IENGINE Common、IENGINE App 全部编译成功，`BUILD SUCCESS`。

覆盖证据：暂停快照保持不变、普通 Source 未启动、事件顺序保持、普通 sink 不被调用、replay source 与上下文资源均按逆序关闭，并保留 live 协调器原有测试通过。

## 后续约束

E06 必须把 `DqlReplaySourceNode` 绑定到原任务 DAG 的真实 Source 边界；不能将 runner 改成直接调用 target writer，也不能为了回放把暂停任务状态改成 running。
