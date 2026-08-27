# A04 冻结异常分类规则

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依据：详细设计第 8.1-8.5、10 和 17.1 节

## 分类输出

分类器固定输出：

- `exceptionScope`：`RECORD`、`TASK_SHARED`、`SYSTEM`、`UNKNOWN`。
- `routeDecision`：`RECORD_DLQ`、`TASK_RETRY`、`TASK_ERROR`。
- `errorType`：只在记录级 DQL 事件中使用。
- `classificationReason`：记录命中的错误码、异常链、节点和阶段摘要。
- `classificationConfidence`：`EXACT`、`RULE`、`UNKNOWN_SINGLE`。

## 路由矩阵

| 异常场景 | Scope | Decision | 是否写入 DQL |
| --- | --- | --- | --- |
| 字段类型、长度、日期格式、非空、唯一约束等单记录错误 | `RECORD` | `RECORD_DLQ` | 是 |
| JS 或自定义处理节点对单条 DML 执行失败 | `RECORD` | `RECORD_DLQ` | 是 |
| 可定位单条记录但错误码未知，且未触发保护 | `UNKNOWN` | 经保护器允许后按 `RECORD_DLQ` | 是，类型为 `UNKNOWN_RECORD_ERROR` |
| 网络抖动、连接超时、连接拒绝、连接池耗尽、数据库临时不可用 | `TASK_SHARED` | `TASK_RETRY` | 否 |
| 共享故障重试耗尽或持续不可用 | `TASK_SHARED` | `TASK_ERROR` | 否 |
| TM 不可用、线程中断、OOM、任务停止、进程关闭 | `SYSTEM` | `TASK_ERROR` | 否 |
| 账号权限、表不存在、任务配置非法、脚本引擎初始化失败 | `SYSTEM` | `TASK_ERROR` | 否 |
| 源解析阶段无法构造 `TapRecordEvent` | `SYSTEM` 或 `TASK_SHARED` | `TASK_ERROR` 或 `TASK_RETRY` | 否 |

## 批量保护规则

- 批量写入失败时先做批量级分类；共享异常不得先拆单再逐条入 DQL。
- 单条失败仍需再次分类，只有最终 `RECORD_DLQ` 才允许上报。
- 未知异常按 `taskId + failedNodeId + tableId + errorCode + normalizedErrorMessage` 建立窗口。
- 窗口内未超过阈值时允许有限未知单记录进入 DQL。
- 超过数量阈值或批次比例阈值后转 `TASK_RETRY`，停止继续生成 DQL，并记录日志和告警。

## 强制失败语义

- DQL 上报成功后才允许 skip。
- DQL 上报失败、Payload 摘要也无法保存、达到跳过限制或触发 Storm Guard 时不得 skip。
- 路由为 `TASK_RETRY` 时必须抛回现有任务错误路径，由 `TaskRetryService` 决定重试或耗尽。
- 路由为 `TASK_ERROR` 时直接进入现有不可跳过错误路径。

## 已识别的代码跟进

- 当前 `DqlErrorTypeEnum.TARGET_WRITE_ERROR` 与设计的 `TARGET_CONSTRAINT_ERROR` 命名不一致，B01 必须统一。
- C04-C12 负责实现和验证本契约；不得用字符串包含关系作为唯一分类依据。
