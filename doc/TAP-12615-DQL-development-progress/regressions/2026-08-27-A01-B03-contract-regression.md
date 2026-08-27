# A01-B03 最新契约回归记录

## 回归信息

- 日期：2026-08-27
- 分支：`TAP-12615-DLQ-4.22`
- 范围：A01-A05、B01-B03
- 最新依据：`doc/TAP-12615-DLQ-controlled-reprocessing-api.md`
- 边界：不开发 B04 及后续步骤，只修正已完成范围内被最新前端交互和 API 契约影响的实现。

## 回归结论

| 步骤 | 检查结果 | 本次同步 |
| --- | --- | --- |
| A01 | 通过 | DQL/DLQ 命名、集合和路径前缀不变 |
| A02 | 需同步 | Attempt 结果增加 `RUNNING`；B03 批次终态来源按冻结状态机收紧 |
| A03 | 需同步 | 当前 Web 依赖收敛为列表、汇总、详情、预览、提交 5 个接口；批次详情为可选诊断；补齐目标节点字段链路 |
| A04 | 需同步 | `TARGET_WRITE_ERROR` 改为规范值，历史 `TARGET_CONSTRAINT_ERROR` 兼容输入后归一化 |
| A05 | 通过 | 本次契约调整未改变配置默认值和 POC 环境要求 |
| B01 | 已修正 | 枚举、上报 VO、事件 DTO/Entity、服务转换和错误类型规范化已同步 |
| B02 | 已修正 | 默认分页从 10 调整为 20；目标节点字段加入 insert-only upsert |
| B03 | 已修正 | 拒绝通用状态更新写入非 `DISPATCHED`；拒绝非终态 finish，并限制各终态来源状态 |

## 代码同步

- `DqlErrorTypeEnum` 使用 `TARGET_WRITE_ERROR`；解析旧 `TARGET_CONSTRAINT_ERROR` 时返回规范枚举。
- `DqlEventService` 在持久化前规范化已识别的错误类型，避免新增数据继续写入旧值。
- `DqlRecoveryAttemptResultEnum` 增加 `RUNNING`。
- `DqlEventReportVo`、`DqlEventDto`、`DqlEventEntity`、服务转换和 Repository 写入链路增加 `targetNodeId`、`targetNodeName`。
- `DqlEventRepository` 未传正数 `limit` 时默认查询 20 条。
- `DqlRecoveryBatchRepository` 将通用状态更新和 finish 限制为 A02 允许的迁移。

## TDD 与验证

- 修改前基线：4 组定向测试共 36 个测试通过。
- 红灯一：新契约测试因缺少 `TARGET_WRITE_ERROR` 和 `RUNNING` 编译失败。
- 红灯二：服务测试确认旧 `TARGET_CONSTRAINT_ERROR` 会被原样持久化，而不是规范化为 `TARGET_WRITE_ERROR`。
- 最终定向回归：`DqlEnumContractTest` 4 个、`DqlEventControllerTest` 8 个、`DqlEventServiceTest` 16 个、`DqlEventRepositoryTest` 11 个、`DqlRecoveryBatchRepositoryTest` 10 个，共 49 个测试全部通过；Maven reactor 7 个模块构建成功。

## 明确延后

- B09：内部 `failed_stage` 对外映射为 `stage`，内部 attempt `error_details` 对外映射为 `errorMessage`，并保证详情适配 3 秒刷新。
- D01：`blockedEvents` 补齐表名、DML、事件时间、捕获序号和用户可读 message。
- D06：创建和推进 `RUNNING` attempt、回调幂等及事件/批次计数一致性。
- B04 及其后的其他功能不在本次回归范围内。

## Review 停止点

本次回归完成并形成本地提交后暂停。待人工 review 和验证通过，再继续后续步骤开发。
