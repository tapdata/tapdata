# A01 冻结 DQL、DLQ 对外命名

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依据：详细设计第 1、2、4、6、7、12 节

## 完成内容

本步骤冻结以下命名规则：

| 使用位置 | 固定命名 | 说明 |
| --- | --- | --- |
| Java 包和类前缀 | `dql`、`Dql` | 与现有 `com.tapdata.tm.dql` 保持一致 |
| Mongo 集合 | `dql_events`、`dql_recovery_batches` | 不使用历史 skip 日志集合代替 |
| Mongo 字段 | snake_case | 例如 `task_id`、`event_time`、`ttl_at` |
| Java/API 字段 | camelCase | 例如 `taskId`、`eventTime`、`ttlAt` |
| TM API 路径 | `/api/dql-events` | Engine 内部回调使用 `/api/task/{taskId}/dql-events/...` |
| Engine 消息类型 | `dqlRecovery` | 独立于 `DataSyncMq.OP_TYPE_*` |
| 用户可见名称 | 异常事件 | 不向用户暴露 DQL 缩写作为主要页面名称 |
| 概念说明 | DLQ | 文档中用于描述 Dead Letter Queue 设计概念 |

## 约束

- 不新增 `dlq_events` 等同义集合或第二套 API。
- 现有表级跳过能力 `TaskSkipErrorTable` 不改名，也不并入 DQL 集合。
- Web 由其他人员负责；TM 需要交付稳定的 API 和权限码命名。

## 验证结果

- 当前 TM 包、集合、API 和新增 TTL 脚本符合上述命名。
- 未发现需要阻塞后续开发的命名冲突。

## 后续影响

- B01-B11、C01、D05 和 E01 必须沿用本契约。
- 任何对外字段或 API 改名必须同步接口文档并通知 Web 负责人。
