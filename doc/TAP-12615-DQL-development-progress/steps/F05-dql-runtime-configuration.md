# F05 DQL 系统配置及读取逻辑

## 步骤信息

- 状态：已完成（待集成验证）
- 完成日期：2026-08-28
- 依赖：A05

## 结论

F05 已完成。DQL 的 14 个运行时配置项由 `tm-common` 中的 `DqlRuntimeConfig` 统一定义默认值、读取优先级和合法范围；TM 与 Engine 不再各自维护一份可变的 DQL 限制常量。

读取优先级固定为：JVM system property > 环境变量（原 key 或大写下划线形式）> Settings/SettingService > 代码默认值。空值、格式错误和越界值按单项回退到默认值，不会以不安全值启动 DQL 流程。

## 配置接入

| 配置组 | 接入位置 | 生效方式 |
| --- | --- | --- |
| 事件开关、Payload 和预览限制 | Engine `SkipErrorEventAspectTask` | 任务启动时读取快照 |
| Storm Guard 窗口、数量、比例和路由 | Engine `DlqStormGuard` | 任务启动时读取快照 |
| 上报错误详情、Payload 和预览安全边界 | TM `DqlReportValidationService` | 每次 TM 上报按当前安全配置读取 |
| 恢复批次上限、批次超时和任务锁租期 | TM `DqlRecoveryBatchService`、`DqlRecoveryTaskLockRepository` | 新预览/新批次读取最新配置 |
| 恢复消息批次上限 | Engine `DqlRecoveryEventHandler` / `DqlRecoveryMessageHandler` | Handler 初始化时形成快照 |
| 恢复事件超时、单事件失败继续策略 | Engine `DqlRecoveryCoordinatorImpl` 配置构造入口 | Coordinator 创建时形成快照 |

恢复事件 `TapdataDqlRecoveryEvent` 和 `DqlRecoveryCoordinatorImpl` 增加了带配置快照的构造入口，保证回放反序列化和屏障策略可使用同一组配置；原有无配置构造方法保留默认值兼容。

## 初始化

`manager/tm/src/main/resources/init/idaas/4.22-7.json` 增加 14 项 `Settings` 幂等 upsert：

`dql.event.enabled`、`dql.event.errorDetails.maxLength`、`dql.event.payload.maxBytes`、`dql.event.preview.fieldMaxLength`、`dql.event.preview.maxDepth`、`dql.event.preview.maxItems`、`dql.recovery.batch.maxSize`、`dql.recovery.eventTimeoutSeconds`、`dql.recovery.batchTimeoutSeconds`、`dql.recovery.continueOnEventFailure`、`dql.unknown.guard.windowSeconds`、`dql.unknown.guard.maxEvents`、`dql.unknown.guard.maxBatchRatio`、`dql.unknown.guard.decision`。

配置均为 `System`、全局、非用户可见且关闭热加载。Engine 任务启动及 recovery Handler/Coordinator 创建时形成不可变快照；TM 的上报安全校验和新预览/批次/超时扫描操作按当前 Settings 读取并校验，因此配置变更不会中途替换已创建的 Engine 回放策略。初始化只使用 `$setOnInsert.value` 写入默认值，升级不会覆盖已有用户设置。

## 验证记录

通过：

```text
mvn -o -pl manager/tm-common \
  -Dtest=DqlRuntimeConfigTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：4/4，覆盖 14 项默认值、全部合法覆盖、非法值逐项回退和 system property 优先级。

通过：

```text
mvn -o -pl manager/tm -am \
  -Dtest=DqlInitializationPatchTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：1/1，覆盖 14 项 Settings 初始化、默认值、幂等 upsert 和不可见/非热加载属性。

通过：

```text
mvn -o -pl iengine/iengine-app -am \
  -Dtest=DqlPayloadSerializerTest,DqlPayloadPreviewBuilderTest,DlqStormGuardTest,\
DqlRecoveryMessageParserTest,DqlRecoveryMessageHandlerTest,DqlRecoveryCoordinatorImplTest,\
DqlRecoveryReplayRegressionTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果：Engine common 23/23，Engine app 22/22；包含新配置构造入口的 Coordinator 超时/继续策略测试。

TM 配置接入源码编译通过。合并执行 TM 既有 DQL 测试时，当前 JetBrains JDK 17 无法让 Mockito inline/Byte Buddy self-attach，导致 45 个测试错误；失败发生在 mock 初始化，不是 F05 断言失败，已作为环境限制保留给 F06/F07 复核。

## 后续依赖

F06 验证初始化脚本在空库、已有集合、重复执行、索引已存在和历史数据缺少 `ttl_at` 时的兼容性；F07 验证未开启 DQL/SkipData 的旧任务路径不受影响。
