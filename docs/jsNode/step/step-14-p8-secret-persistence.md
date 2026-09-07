# TAP-12832 开发步骤 14：脚本参数密文持久化

## 本步骤目标

完成 `scriptParams.encrypted=true` 参数在 TM 保存链路中的密文落库，运行时仍由引擎现有 AES256 解密。覆盖常规任务保存和单独 DAG 更新接口，避免用户编辑任务时对已有密文重复加密。

## 代码变更

- 新增 `com.tapdata.tm.utils.JsNodeConfigSecretUtil`。
  - 使用 TM 与引擎共用的 AES256/ECB/PKCS5 格式。
  - STRING、NUMBER、BOOLEAN 使用字符串表示；JSON 非字符串值先序列化为 JSON。
  - 对合法 AES 十六进制密文做幂等识别，避免重复加密。
- `TaskServiceImpl.beforeSave` 在节点入库前处理脚本参数密文。
- `TaskServiceImpl.updateDag` 补充同一处理，覆盖 `dag` 和 `dagNotHistory` 接口。
- 增加工具单元测试以及 TM 保存前处理的回归测试。

## 验收结果

| 检查项 | 结果 |
| --- | --- |
| 明文参数加密后可由现有 AES256 解密 | 通过 |
| 已有密文重复保存不发生二次加密 | 通过 |
| JSON 参数加密前保持有效 JSON | 通过 |
| `manager/tm-api` 加密工具测试 | `mvn -q -pl manager/tm-api -Dtest=JsNodeConfigSecretUtilTest test` 通过 |
| `manager/tm-common` 节点模型回归 | `mvn -q -pl manager/tm-common -Dtest=JsNodeConfigParamTest test` 通过 |
| `manager/tm` 保存链路测试 | 当前仓库存在与本步骤无关的 SSO 源码缺失，Maven 在测试编译前失败；已保留测试用例，待基线修复后执行 |

## 边界说明

- 密文格式沿用现有固定 AES256 密钥和十六进制编码，不新增第二套密钥管理。
- 加密只发生在 TM 写入任务 DAG 前；引擎只在授权的正式运行上下文解密，试运行仍按 dry-run/无 secret resolver 规则处理。
- 密文幂等判断依赖 AES 十六进制密文形态；参数值仍需经过 `JsNodeConfigValidator` 的类型和大小校验。
- 本步骤不改变 API 返回结构，也不在日志中打印参数值。
