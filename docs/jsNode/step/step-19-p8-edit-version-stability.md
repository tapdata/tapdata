# TAP-12832 开发步骤 19：密文参数与编辑版本一致性

## 本步骤目标

避免脚本参数在 `buildEditVersion` 之后才加密，导致首次保存使用明文 hash、后续编辑使用密文 hash，从而对没有实际改动的任务产生版本变化或并发编辑冲突。

## 代码变更

- 创建任务计算 `editVersion` 前先对 DAG 中 `encrypted=true` 参数执行幂等加密。
- 更新任务计算 `editVersion` 前执行同样的归一化。
- 抽取 DAG 级加密遍历方法，供 `updateDag`、常规保存和独立 DAG 更新共用。
- `beforeSave` 仍保留最后一道幂等保护，覆盖其他内部调用方。

## 验收结果

| 检查项 | 结果 |
| --- | --- |
| 代码差异检查 | `git diff --check` 通过 |
| AES 密文幂等行为 | 由 `JsNodeConfigSecretUtilTest` 覆盖并通过 |
| 节点保存/更新编译验证 | 受仓库已有 TM SSO 缺失类阻塞，待基线恢复后执行 |

## 边界说明

- 版本 hash 仍由现有 `buildEditVersion` 负责，本步骤只保证加密参数在 hash 前使用统一表示。
- 不加密的参数类型和旧任务没有 `scriptParams` 的行为不变。
- 已有密文不会二次加密；新明文只在 TM 内存对象进入 hash 前转换为密文。
