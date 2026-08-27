# E07：逐事件恢复屏障与完成判定

## 结论

E07 已完成。协调器在每条 `TapdataDqlRecoveryEvent` 注入后，向同一个 source boundary 追加一个 `TapdataCountDownLatchEvent`，并等待该事件在下游目标/处理链完成。等待结果只接受一次终态：目标成功为 `SUCCESS`，恢复失败回调为 `FAILED`，等待超时为 `TIMEOUT`。上一条事件未完成前，协调器不会注入下一条事件。

## 代码产出

- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryBarrierCoordinator.java`
  - 以 event ID 建立 pending barrier，防止同一事件重复建立屏障。
  - 在 source boundary 追加唯一的 `TapdataCountDownLatchEvent`，支持同步完成和异步完成。
  - `complete` 使用 first-terminal-wins 语义，目标失败可唤醒等待线程并返回 `FAILED`。
  - 超时返回 `TIMEOUT`，并在 `finally` 删除本地 pending 状态，避免批次长期泄漏。
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlReplaySourceNode.java`
  - 增加可选的 `enqueueBarrier` source-boundary 能力；不支持屏障的旧边界显式失败，避免静默绕过顺序保证。
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlSourceBoundaryInjector.java`
  - 将屏障事件转发到已由 E06 解析出的真实 DAG source boundary。
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryOnlyRunner.java`
  - 实现 `DqlReplaySourceNode`，将屏障事件转发到暂停任务的 recovery-only source；不创建正常 Source reader。
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
  - 增加 per-batch `BarrierFactory`，live 和 paused 两种源边界都可以绑定真实屏障实现。
  - 保留 E03 兼容屏障端口；未配置新工厂时继续使用注入的旧 `DqlRecoveryBarrier`。
  - 每条事件严格执行“读取快照 → 注入 DML → 注入屏障并等待 → 上报 EVENT_RESULT”的顺序。

现有 Engine 目标/处理链已具备 `TapdataCountDownLatchEvent` 的计数处理分支，本步骤只补齐 DQL source boundary 到该完成信号的编排和失败回调接口，没有增加目标旁路写入。

## TDD 与验证结果

### 测试矩阵

新增 `DqlRecoveryBarrierCoordinatorTest`，覆盖：

- DML 后只追加一个屏障，并在完成后清理 pending 状态；
- 匹配事件的失败完成返回 `FAILED`；
- 未收到完成信号时返回 `TIMEOUT` 且不残留屏障状态。

同时回归 E03 协调器顺序/策略测试和 E06 source boundary 注入测试。

### GREEN

执行：

```bash
mvn -o -pl manager/tm-common,iengine/iengine-common,iengine/iengine-app -am \
  -Dtest=DqlRecoveryBarrierCoordinatorTest,DqlRecoveryCoordinatorImplTest,DqlSourceBoundaryInjectorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：12 个测试通过，0 failures，0 errors；TM Common、IENGINE Common、IENGINE App 全部编译成功，`BUILD SUCCESS`。仓库仍输出既有重复依赖、过时 API、SLF4J provider 和 JaCoCo 执行数据警告，本步骤未引入新的编译错误。

### 调试记录

首次 E07 编译指出暂停任务 runner 尚未实现新增的 source-boundary 屏障能力；随后将 runner 扩展为 `DqlReplaySourceNode` 并保持其 recovery-only 约束，重新编译和测试全部通过。该修复没有放宽快照校验，也没有改变暂停任务状态。

## 后续依赖

E08 需要在已有 processor/target 错误捕获入口识别 recovery 标记，把失败转为原事件 attempt 结果和屏障失败，而不是再次创建 DQL 主记录。E09 继续负责 coordinator、source gate、runner 和回调失败时的批次补偿与资源释放。
