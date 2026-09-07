# TAP-12832 P8：脚本 API 模板

## 完成内容

- JS 编辑器增加“插入配置 API”按钮，插入 `jsNodeConfig.get('key')` 示例。
- 增强 JS 节点增加“插入文件复制 API”按钮，插入 `ftp.copyByConfig` 的最小调用结构；标准 JS 节点不展示文件模板。
- 模板使用 `mgm.in`、`mgm.out` 等可替换前缀和路径占位值，不把 FTP 属性写死在表单中。
- 抽取 `insertScriptApiTemplate` 纯函数，确保模板插入到 `process` 函数右括号前，不破坏已有脚本。

## 自主审核与验证

- `node --test packages/dag/src/components/form/js-node-config-editor/script-params.test.js`：5 个测试通过。
- `pnpm exec prettier --check ...`：相关文件通过。
- `pnpm exec vue-tsc -p packages/dag/tsconfig.json --noEmit`：模板改动未引入新增类型错误；处理器文件仍显示仓库既有类型问题。

## 边界说明

- 模板只负责插入示例文本，不自动创建参数、不自动保存 FTP 配置，也不自动执行远端操作。
- `ftp` 只有在节点参数包含协议配置并且运行时成功装载共享服务时才注入；否则脚本调用会得到服务不可用错误。
