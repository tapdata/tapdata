# TAP-12832 开发步骤总结：P4 节点参数模型与基础校验

## 本步骤完成内容

- 在 `ScriptProcessNode` 增加 `@EqField List<JsNodeConfigParam> scriptParams`，缺省值为空列表。
- 在独立的 `MigrateScriptProcessNode` 脚本基类同步增加 `scriptParams`，避免迁移 JS 节点序列化时丢失配置。
- 增加 `JsNodeConfigParam`：支持 `key`、`type`、`value`、`encrypted`、`description`。
- 增加 `JsNodeConfigValueType`：`STRING`、`NUMBER`、`BOOLEAN`、`JSON`，JSON 序列化使用小写名称，同时兼容大写输入。
- 增加 `JsNodeConfigValidator` 和 `JsNodeConfigValidationError`，实现 key 唯一性、格式、保留前缀、类型、JSON 格式、单值/总量/参数数量上限校验。
- setter 将显式 `null` 参数列表归一化为空列表，保证旧 DAG 和缺省字段兼容。

## 测试与自审

新增 `JsNodeConfigParamTest`，覆盖：

- 普通 JS 节点参数 JSON 序列化和反序列化。
- 迁移 JS 节点参数 JSON 序列化和反序列化。
- 枚举大小写兼容。
- 有效 string/number/boolean/json 参数。
- 重复 key、保留前缀、类型错误、非法 JSON、空列表和超大值的字段级错误。

执行结果：

- 先运行新增测试，因模型和校验器不存在而编译失败，确认红灯。
- `mvn -q -Dtest=JsNodeConfigParamTest test`：通过。
- `mvn -q test`（`manager/tm-common`）：通过；仓库既有 JUnit discovery 警告不影响结果。

## 当前边界

- 本步骤只完成持久化模型和纯校验，不负责密文解密、secret 权限和运行时访问器；这些属于 P4-03/P5。
- `encrypted=true` 只要求持久化值为字符串并阻止普通类型校验，不在公共 DTO 中引入新的密钥系统。
- 校验器限制参数最多 100 个、单值 64 KB、总值 512 KB；JSON 深度和 secretRef 语义由后续运行时安全层继续约束。

## 下一步

执行 P4-03：接入现有密文/secret 权限能力，确保无权限用户不能启动依赖敏感参数的任务，且导出、diff、日志和错误信息不泄露明文。
