# TAP-12832 P8：节点配置保存前校验

## 完成内容

- `ScriptProcessNode` 和 `MigrateScriptProcessNode` 的 `validate()` 接入 `JsNodeConfigValidator`。
- DAG 模型校验阶段会拒绝重复 key、非法 key、类型不匹配、超限值和非法 JSON；空参数列表和旧节点继续通过。
- 新增普通 JS 节点和迁移 JS 节点的校验回归测试。

## 自主审核与验证

- `mvn -q -pl manager/tm-common -Dtest=JsNodeConfigParamTest test`：通过。

## 边界说明

- 节点层校验返回布尔值，具体字段错误仍由 `JsNodeConfigValidator` 提供；API 层可在后续接口中映射为字段级响应。
- Python 脚本节点复用基类，但没有参数时不改变原有行为；若未来启用相同参数字段，会自动获得同一校验规则。
