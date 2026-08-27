# B01 补齐 TM 领域枚举、DTO、VO 和 Entity 契约

## 步骤信息

- 状态：已完成
- 完成日期：2026-08-27
- 依赖：A02、A03

## 完成内容

- 新增 `DqlExceptionScopeEnum`：`RECORD`、`TASK_SHARED`、`SYSTEM`、`UNKNOWN`。
- 新增 `DqlRouteDecisionEnum`：`RECORD_DLQ`、`TASK_RETRY`、`TASK_ERROR`。
- 新增 `DqlClassificationConfidenceEnum`：`EXACT`、`RULE`、`UNKNOWN_SINGLE`。
- 将 `DqlErrorTypeEnum` 的设计值统一为 `TARGET_CONSTRAINT_ERROR`，`parse()` 对历史 `TARGET_WRITE_ERROR` 保留兼容映射。
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
- API 文档中旧的 `TARGET_WRITE_ERROR` 只能作为兼容输入，新的输出和存储值使用 `TARGET_CONSTRAINT_ERROR`。
