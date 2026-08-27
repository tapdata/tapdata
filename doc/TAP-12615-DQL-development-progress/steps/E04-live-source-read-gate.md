# E04：运行中任务 Source 读取闸门

## 结论

E04 已完成。闸门位于 `HazelcastSourcePdkBaseNode.enqueue` 的统一 Source 入队入口，不改变 `TaskDto.status`，通过四态状态机控制普通事件与 DQL recovery/barrier 事件的准入：

```text
OPEN -> PAUSING -> RECOVERY_ONLY -> RESUMING -> OPEN
```

普通事件只在 `OPEN` 状态进入队列，并在 `enqueue` 返回前标记为已完成；进入 `PAUSING` 后拒绝新的普通事件，调用方可等待已接受的 Source 入队操作排空，再切换到 `RECOVERY_ONLY`。恢复事件和 `TapdataCountDownLatchEvent` 只在 `OPEN` 或 `RECOVERY_ONLY` 状态允许通过，避免回放期间混入新的普通 Source 事件。

## 代码路径

- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlSourceReadGate.java`
  - 使用 `ReentrantLock`、`Condition` 和 identity set 跟踪在途普通事件。
  - 提供 `open`、`beginPausing`、`enterRecoveryOnly`、`beginResuming`、`awaitDrained`、`allow`、`release`、`close` 生命周期 API。
  - `close()` 委托到 `open()`，保证正常结束和异常结束都恢复普通 Source 准入。
- `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastSourcePdkBaseNode.java`
  - `enqueue` 首行调用 `allow`，拒绝事件时不触碰原有队列逻辑。
  - 原有表过滤、CDC 过滤和队列 offer 逻辑保持不变，并在 `finally` 中调用 `release`。
  - 暴露 `getDqlSourceReadGate()` 供后续 E05-E09 的 live-task runner、恢复和补偿流程接入。
  - `doClose()` 的 finally 中恢复闸门，不修改任务业务状态。

## TDD 证据

### 红测

先执行：

```bash
mvn -o -pl manager/tm-common,iengine/iengine-common,iengine/iengine-app -am \
  -Dtest=DqlSourceReadGateTest -Dsurefire.failIfNoSpecifiedTests=false test
```

结果为 testCompile 失败，编译器报告 `DqlSourceReadGate` 类型不存在，证明测试先于实现暴露了缺口。

### 绿测

实现后执行：

```bash
mvn -o -pl manager/tm-common,iengine/iengine-common,iengine/iengine-app -am \
  -Dtest=DqlSourceReadGateTest,DqlRecoveryCoordinatorImplTest,DqlRecoveryMessageHandlerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：12 个测试通过，0 failures，0 errors；TM Common、IENGINE Common、IENGINE App 全部编译成功，`BUILD SUCCESS`。

本步骤未发现独立的生产 Source 节点 enqueue 单测；因此额外完成了 `HazelcastSourcePdkBaseNode` 的编译回归，并保留闸门行为的独立并发测试，后续 E06 源边界注入和 E10 端到端回放矩阵继续覆盖真实 Source 路径。

## 后续约束

E05-E07 接入时必须遵循 `PAUSING` 后先 `awaitDrained` 再 `enterRecoveryOnly` 的顺序；不能通过修改任务状态或直接调用目标节点绕过该边界。恢复失败时由 E09 在 finally 中调用 `close()`。
