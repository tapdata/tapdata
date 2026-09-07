# TAP-12832 P8：运行期密文类型转换

## 完成内容

- 修正 `DefaultJsNodeConfigAccessor`：加密值通过授权 secret resolver 解密后，按照参数声明类型转换。
- `NUMBER` 支持整数和小数，`BOOLEAN` 只接受 `true/false`，`JSON` 继续由 Jackson 解析，非法值返回 `JS_NODE_CONFIG_TYPE_INVALID`。
- 明文参数的既有类型行为保持不变，密文原文仍不会进入异常消息。

## 自主审核与验证

- 先新增 `encryptedPrimitiveValuesAreConvertedAfterDecryption` 测试，确认旧实现会发生 `String` 到 `Number` 的类型错误。
- 修复后 `mvn -q -pl iengine/iengine-common -Dtest=JsNodeConfigAccessorTest test` 通过。
- `mvn -q test`（`iengine-common`）通过；输出中的 Hazelcast/Mongo 测试日志为既有测试行为，没有失败。

## 边界说明

- 加密值没有 secret resolver 时仍拒绝访问，不会退回返回密文。
- 数字转换返回 `Integer`、`Long` 或 `Double`，脚本侧只依赖 JavaScript 数字语义。
- 该转换只发生在引擎内存中，持久化对象和前端仍保存加密标记及密文载荷。
