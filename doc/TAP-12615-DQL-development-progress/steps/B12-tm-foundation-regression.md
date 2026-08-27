# B12 TM 基础能力回归与补齐

## 状态

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：B01-B11
- 范围：TM DQL 基础能力和 TM API 错误语义的自动化回归，不包含 Engine 功能开发或 Web 页面实现

## 目标

本步骤针对 B01-B11 已完成实现进行一次整体回归，补齐此前缺少的批次 Service、Web 映射和 DTO/Entity 映射测试，并按最新前端交互与 API 数据契约验证实现。重点检查正常路径、关键失败路径、幂等/补偿、安全输出和状态计数语义。

## 覆盖内容

### 重处理批次 Service

- `SKIPPED` 结果只增加 skipped 计数，不增加 failed 计数，也不触发失败告警。
- 预览按服务端结果返回阻塞事件，并输出最新契约要求的 `message`、源表、目标表、DML、事件时间和捕获序号。
- 发起批次使用服务端有序事件列表下发 `dqlRecovery` 消息。
- 事件锁数量不匹配、消息下发失败时，批次失败并释放事件锁。
- `BATCH_FAILED` 回调释放锁、收敛批次状态并触发失败告警。

### Web 映射与安全边界

- 列表响应只暴露公开摘要字段，不输出 `payloadData`、事件身份、记录身份和内部错误详情。
- 详情响应对嵌套预览递归脱敏，长字符串截断，attempt 按最新顺序限制为 20 条，并将错误详情映射为错误消息。
- 保留空 attempt 列表与空预览的区别，避免前端误判数据状态。

### DTO/Entity 与 Repository 映射

- 事件 Entity 到 DTO 的 ObjectId 转换、状态、Payload、当前批次和 attempt 字段保持一致。
- 批次创建将 DTO 字段完整映射到 Entity，保留标识并验证 `ttl_at` 与创建时间一致。
- 已有 Repository、Controller、权限、TTL、并发条件更新和错误语义测试纳入同一回归集。

## 回归中发现并修正的问题

1. 批次结果处理原先把 `SKIPPED` 走到了失败分支，导致 failed 计数增加；已增加独立 skipped 分支并保持事件终态更新。
2. 阻塞事件 JSON 仍输出旧字段 `reason`，且缺少前端展示上下文；已切换为 `message` 并补齐上下文字段。Java 侧保留 `getReason/setReason` 的忽略序列化别名，兼容既有调用方。

## 代码与文档产出

- `manager/tm/src/main/java/com/tapdata/tm/dql/service/DqlRecoveryBatchService.java`
- `manager/tm-common/src/main/java/com/tapdata/tm/dql/vo/DqlRecoveryPreviewVo.java`
- `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlRecoveryBatchServiceTest.java`
- `manager/tm/src/test/java/com/tapdata/tm/dql/service/DqlEventWebMapperTest.java`
- `manager/tm/src/test/java/com/tapdata/tm/dql/repository/DqlEntityMappingTest.java`
- 本步骤文档及开发计划、进度索引同步更新

## 验证结果

使用离线 Maven 依赖并串行执行，Mockito 使用本地 Java agent：

```text
TM A01-B12 DQL 回归：Tests run: 110, Failures: 0, Errors: 0, Skipped: 0
TM API ExceptionHandler 回归：Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
两次构建均 BUILD SUCCESS
```

同时执行 `git diff --check`，未发现空白或补丁格式问题。

## 后续依赖

B12 完成后暂停开发，等待本步骤代码和契约回归 review。后续进入 C01 前，应继续以最新 API 文档为准完成 Engine DQL 公共模型与 TM Client，并在 D01-D10 开发前复核批次控制契约。
