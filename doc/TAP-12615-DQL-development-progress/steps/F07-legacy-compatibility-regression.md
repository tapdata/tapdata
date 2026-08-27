# F07 前后兼容回归

## 状态

- 状态：已完成（待集成验证）
- 完成日期：2026-08-28
- 依赖：C12、E10、F01-F06

## 分析结论

F07 的兼容边界不是重新实现 SkipErrorTable，而是确保 DQL 新路径不会改变旧任务的路由和告警语义。检查结果如下：

1. Engine 的数据捕获、目标写入后处理和处理节点失败三个入口现在统一受系统级 `dql.event.enabled` 保护。DQL 关闭或运行时配置未初始化时，入口保持旧路径，不会因残留 handler wiring 产生 DQL 事件。
2. `SkipTable`、`Disable` 和 `SkipData` 不会创建 `TaskSkipErrorTable`；`SkipTableForMigrateSnapshot` 仍只对迁移任务生效。既有 `SkipErrorTable` 的全量/增量表状态判断逻辑未修改。
3. 共享网络异常仍由任务级重试和原有任务告警处理；C12 已覆盖共享异常不进入 DQL 的回归约束。DQL 告警为追加能力，未替换 `TASK_STATUS_ERROR` 等旧任务告警路径。
4. F01-F06 引入的权限、告警、配置、初始化和索引变更均不改变旧 API 的返回语义；真实 TM/Engine/Mongo 环境中的升级、通知渠道和存量任务验证仍由 G05、G08、G09、G12 承接。

## 代码产出

- `SkipErrorEventAspectTask` 的三个 DQL 捕获入口增加统一的系统开关保护。
- 新增 Engine 兼容回归测试，覆盖系统开关关闭、SkipTable、SkipTableForMigrateSnapshot 和 Disable 场景。
- 新增 TaskSkipErrorTable 工厂的 legacy/DQL 模式回归测试，覆盖禁用、SkipTable、SkipData 以及非迁移任务。

## 验证结果

- `F07EngineCompatibilityRegressionTest`：2/2 通过。
- `C12EngineCaptureRegressionTest`：3/3 通过；C12 文档记录的组合回归 20/20 通过。
- `F07SkipErrorTableCompatibilityTest`：2/2 通过。
- 现有 `SkipErrorTableTest` 继续作为旧表语义基线；本次未改变其生产实现。该测试在当前 JetBrains JDK 17 下直接运行会触发仓库既有的 Mockito inline agent 自附加问题，不能将此环境问题计为业务失败。

## 后续依赖

F07 代码级兼容检查完成，M5 可以关闭为“已完成（待集成验证）”。真实升级脚本、存量任务、通知渠道和端到端旧流程需要在 G 阶段环境中验证。
