# M0 契约冻结里程碑总结

## 里程碑信息

- 状态：已完成
- 完成日期：2026-08-27
- 包含步骤：A01-A05

## 完成结果

| 领域 | 冻结结果 |
| --- | --- |
| 命名 | 内部代码和集合使用 DQL，用户可见名称使用“异常事件”，概念文档使用 DLQ |
| 状态机 | 事件、批次、Attempt 和 Engine 回调类型及合法迁移已冻结 |
| 数据与 API | snake_case Mongo、camelCase Java/API、完整 Payload 边界、9 个 API 和独立 `dqlRecovery` 消息已冻结 |
| 异常分类 | RECORD、TASK_SHARED、SYSTEM、UNKNOWN 的路由矩阵和 Storm Guard 行为已冻结 |
| 配置 | 14 个配置 key、默认值、Settings/SettingService 载体和快照生效规则已冻结 |
| POC | 数据类型、异常注入、幂等适用范围和不设置性能阈值的验收口径已冻结 |

## 产出文档

- `steps/A01-naming-contract.md`
- `steps/A02-state-machine-contract.md`
- `steps/A03-cross-component-contract.md`
- `steps/A04-exception-classification-contract.md`
- `steps/A05-configuration-and-poc-contract.md`

## 退出条件检查

- 状态、字段、API、配置和异常分类均有明确实现契约。
- TM 和 Engine 后续开发不再需要自行推断核心语义。
- Web 不在本计划开发范围，但 API、权限码和错误码交接边界已明确。
- 已识别代码与契约差异，并分别指派到 B01、D05、D06、F05 等后续步骤。

## 已知差异

- 当前 `DqlErrorTypeEnum` 使用 `TARGET_WRITE_ERROR`，需在 B01 统一为设计口径。
- 异常范围和路由决策尚缺少强类型枚举，需在 B01 补齐。
- 当前 TM 到 Engine 的恢复消息仍为 Map，需在 D05/E01 改为明确契约模型。
- 配置尚未写入 Settings 初始化，需在 F05 落地。

## 下一里程碑

进入 M1 TM 基础能力，按 B01-B12 顺序复核并补齐已有 TM 实现。
