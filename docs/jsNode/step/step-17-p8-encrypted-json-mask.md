# TAP-12832 开发步骤 17：加密 JSON 参数掩码

## 本步骤目标

保证参数编辑器中所有标记为加密的值都以密码控件显示，覆盖 JSON 类型，避免用户输入密码或 token 时因 JSON 编辑器默认使用 textarea 而直接暴露。

## 代码变更

- `JsNodeConfigEditor` 根据 `encrypted` 优先选择 `password` 类型。
- 加密 JSON 仍可通过控件的显示/隐藏按钮临时查看，未加密 JSON 继续使用 textarea 和自动伸缩。
- 保持 `serializeScriptParams` 对加密值按字符串发送，后端继续负责类型和密文校验。

## 验收结果

| 检查项 | 结果 |
| --- | --- |
| JS 参数工具测试 | `node --test packages/dag/src/components/form/js-node-config-editor/script-params.test.js` 5 项通过 |
| 组件及工具格式检查 | `pnpm exec prettier --check ...` 通过 |
| 代码差异检查 | `git diff --check` 通过 |

## 边界说明

- 掩码只影响编辑器显示，不改变后端密文存储和引擎解密规则。
- 用户主动点击显示按钮后，当前页面可看到值；日志、任务运行结果和脚本 API 仍不会输出配置全集。
- JSON 明文配置继续保留多行编辑体验，加密 JSON 为安全优先使用单行密码控件。
