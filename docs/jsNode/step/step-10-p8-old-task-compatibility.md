# TAP-12832 P8：旧任务参数兼容

## 完成内容

- `DefaultJsNodeConfigAccessor` 对缺失或反序列化为 `null` 的 `scriptParams` 按空列表处理。
- 旧任务无需迁移字段即可继续执行 `jsNodeConfig.has/getOrDefault`，不会在引擎初始化阶段因为参数列表为空而失败。
- 新增回归测试覆盖 `null` 参数列表。

## 自主审核与验证

- `mvn -q -pl iengine/iengine-common -Dtest=JsNodeConfigAccessorTest test`：通过。

## 边界说明

- 参数列表为空时不会创建文件 facade；只有包含协议参数时才装载共享文件服务。
- 旧任务仍不能调用未注入的 `ftp` 对象；这是按配置启用文件能力的预期行为。
