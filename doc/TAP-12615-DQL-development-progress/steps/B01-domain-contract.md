# B01 补齐 TM 领域枚举、DTO、VO 和 Entity 契约

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A02、A03

## 完成内容

- 新增 `DqlExceptionScopeEnum`：`RECORD`、`TASK_SHARED`、`SYSTEM`、`UNKNOWN`。
- 新增 `DqlRouteDecisionEnum`：`RECORD_DLQ`、`TASK_RETRY`、`TASK_ERROR`。
- 新增 `DqlClassificationConfidenceEnum`：`EXACT`、`RULE`、`UNKNOWN_SINGLE`。
- 将 `DqlErrorTypeEnum` 的规范值统一为 `TARGET_WRITE_ERROR`，`parse()` 对历史 `TARGET_CONSTRAINT_ERROR` 保留兼容映射，上报持久化前归一化为规范值。
- `DqlRecoveryAttemptResultEnum` 补齐 `RUNNING`，与详情进度展示契约一致。
- `DqlEventReportVo`、`DqlEventDto` 和 `DqlEventEntity` 补齐 `targetNodeId`、`targetNodeName`，Mongo 字段为 `target_node_id`、`target_node_name`。
- TM 上报服务改用强类型枚举校验 `exceptionScope` 和 `routeDecision`，仍按契约保存大写规范值。
- DTO、Entity、attempt 和 TTL 字段映射已按 A03 的 snake_case/camelCase 规则保留。

## 验证

执行：

```text
mvn -pl tm -am -Dtest=DqlEnumContractTest,DqlEventServiceTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
  -DsurefireArgLine='-javaagent:.../mockito-core-5.20.0.jar' test
```

结果：`DqlEnumContractTest` 3 个测试通过，`DqlEventServiceTest` 15 个测试通过，整个 Maven reactor 编译通过。

## 后续影响

- C04 分类器必须输出上述枚举的合法值。
- D06/D07 回调状态机必须使用批次和报告类型枚举。
- API、Java 和新增存储数据使用 `TARGET_WRITE_ERROR`；历史 `TARGET_CONSTRAINT_ERROR` 只作为兼容输入。
- 对外详情的 `stage` 以及 attempt `errorMessage` 映射仍归 B09，本步骤继续保留内部 `failed_stage` 和 `error_details` 审计字段。
