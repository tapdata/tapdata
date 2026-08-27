# A05 冻结配置和 POC 环境契约

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依据：详细设计第 8.3、15、16、20 节

## 配置项和默认值

| 配置 | 默认值 | 使用方 |
| --- | --- | --- |
| `dql.event.enabled` | `true` | TM、Engine |
| `dql.event.errorDetails.maxLength` | `4000` | TM |
| `dql.event.payload.maxBytes` | `1048576` | Engine、TM |
| `dql.event.preview.fieldMaxLength` | `512` | Engine、TM |
| `dql.event.preview.maxDepth` | `4` | Engine、TM |
| `dql.event.preview.maxItems` | `50` | Engine、TM |
| `dql.recovery.batch.maxSize` | `200` | TM |
| `dql.recovery.eventTimeoutSeconds` | `60` | Engine、TM 补偿参考 |
| `dql.recovery.batchTimeoutSeconds` | `1800` | TM、Engine |
| `dql.recovery.continueOnEventFailure` | `true` | Engine |
| `dql.unknown.guard.windowSeconds` | `60` | Engine |
| `dql.unknown.guard.maxEvents` | `20` | Engine |
| `dql.unknown.guard.maxBatchRatio` | `0.2` | Engine |
| `dql.unknown.guard.decision` | `TASK_RETRY` | Engine |

## 配置载体和生效规则

- 上述配置统一作为系统 Settings 初始化，避免 TM 和 Engine 维护两套默认值来源。
- TM 使用现有 Settings 服务读取；Engine 使用现有 `SettingService` 读取同名设置。
- Settings 缺失、为空或类型非法时使用代码内同值默认常量，并输出一次受控告警日志。
- Engine 捕获配置在任务启动时形成快照；运行中的任务不因 Settings 变化改变当前分类窗口。
- 恢复配置在批次启动时形成快照；活动批次不热切换批次大小、超时或失败继续策略。
- 新启动任务和新批次读取最新设置。

## POC 环境

- 使用同一个开启 SkipData/DQL 的任务同时验证记录级 DQL 和任务级重试。
- 至少准备 Insert、Update、Delete 三类事件。
- 准备 JavaScript 单记录转换失败和目标字段/约束失败场景。
- 准备网络断开、连接拒绝、目标临时停止和持续不可用场景。
- 准备同类未知异常批量触发 Storm Guard 的故障注入方式。
- 测试表必须有主键或唯一业务键，目标写入必须启用幂等或 Exactly-Once。
- 不设置吞吐和延迟验收阈值，只验证路由、状态、顺序、计数、告警和恢复结果。

## 已识别的代码跟进

- F05 负责增加 Settings 初始化、类型校验、默认常量和读取封装。
- G01 负责固化可重复创建的环境、数据集和故障注入步骤。
