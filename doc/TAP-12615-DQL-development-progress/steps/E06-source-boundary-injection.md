# E06：源节点边界注入

## 结论

E06 已完成。DQL 回放入口现在由任务 DAG 决定：只从 `DAG.getSourceNodes()` 中解析 `DataParentNode`，再匹配当前 Engine 已注册的 runtime source boundary。目标节点、处理器节点、未注册的源节点和多个源节点无法明确路由时全部拒绝，不允许通过目标 writer 旁路回放。

单个 `DatabaseNode` 或 `TableNode` 源可以覆盖其配置下的多个表；多源 DAG 暂时要求后续补充明确的表到源节点路由后才能开放，避免将事件静默投递到错误数据源。

## 代码产出

- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlSourceBoundaryInjector.java`
  - 复制 runtime source boundary 映射，防止调用期间被外部修改。
  - 以 DAG 的源节点集合为准，只接受 `DataParentNode` 且已注册的源边界。
  - 暴露 `sourceBoundary()`、`sourceNodeId()` 和 `enqueue(DATA)`；缺失、目标-only 和多源歧义使用确定性异常 fail closed。
  - 保留 E02 `TapdataDqlRecoveryEvent` DATA wrapper，不重新构造或直接操作目标事件。
- `iengine/iengine-app/src/main/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImpl.java`
  - 新增 live `SourceBoundaryFactory`。
  - 暂停任务优先使用 E05 recovery-only runner；运行中任务使用解析出的 DAG source boundary；未配置 factory 时保留既有 sink 兼容端口。
  - live source boundary 由运行中任务拥有，协调器不关闭它，避免回放结束误停正常任务。
- `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlSourceBoundaryInjectorTest.java`
  - 覆盖单源多表、源节点缺失、目标节点不接受注入和多源歧义。
- `iengine/iengine-app/src/test/java/io/tapdata/dql/recovery/DqlRecoveryCoordinatorImplTest.java`
  - 覆盖 live 协调器选择 DAG source boundary，确认 legacy sink 不会被调用。

## 版本与运行时约束

E01 Handler 在协调器启动前已经通过 `MongoDqlRecoveryTaskContextProvider` 校验 task ID、当前发布版本和 Agent；E06 的 `SourceBoundaryFactory` 保留完整 command，因此实际 runtime resolver 必须使用同一份当前版本 DAG 构造 injector。E06 不接受由调用方单独传入目标节点或脱离任务版本的 source map。

E04 的 `DqlSourceReadGate` 仍位于真实 Source 的统一 `enqueue` 入口；E06 只负责把恢复事件送到该源边界，暂停、排空、屏障和恢复准入分别由 E04/E07 负责。

## TDD 与验证结果

### 测试矩阵

实现前先落地 `DqlSourceBoundaryInjectorTest`，固定了单源多表、缺源、目标-only 和多源歧义的行为；实现后增加 live Coordinator 选择测试。缺失源和目标-only 的断言均检查确定性错误信息，避免 future fallback 到 target sink。

### GREEN

执行：

```bash
mvn -o -pl manager/tm-common,iengine/iengine-common,iengine/iengine-app -am \
  -Dtest=DqlSourceBoundaryInjectorTest,DqlRecoveryCoordinatorImplTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：9 个测试通过，0 failures，0 errors；TM Common、IENGINE Common、IENGINE App 全部编译成功，`BUILD SUCCESS`。其中源边界测试 4 项、协调器测试 5 项。

仓库仍输出既有重复依赖、过时 API 和 JaCoCo 执行数据不匹配警告，本步骤未引入新的编译错误。

## 后续依赖

E07 需要在该 source boundary 上注入 `TapdataCountDownLatchEvent` 并等待目标完成回调；E08 必须保证恢复事件在 processor/target 失败时不会再次进入 DQL 捕获；E09 负责 source gate、runner 和回调失败时的清理与批次收敛。
