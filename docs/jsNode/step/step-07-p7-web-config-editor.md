# TAP-12832 P7：前端脚本参数配置编辑器

## 完成内容

1. 在 JS 节点和迁移 JS 节点的配置 schema 中增加 `scriptParams`，参数不是固定 FTP 字段，而是可自由增删的通用键值参数。
2. 新增 `JsNodeConfigEditor` 组件，支持编辑参数名、类型（String/Number/Boolean/JSON）、值、加密标记和说明，支持新增和删除参数行。
3. 新增脚本参数纯函数：
   - 加载时把后端的数字、布尔值、JSON 值转换为可编辑文本。
   - 保存和试运行前按声明类型转换值；加密参数保留密文/待解密文本，不在前端解析。
   - 非法 JSON 保留原始文本，交由后端校验并返回明确错误。
4. JS 节点试运行请求增加 `scriptParams`；节点属性保存前同样执行类型转换，保证任务运行和持久化数据格式一致。
5. 增加中英文及繁体中文文案。

## 代码位置

- `tapdata-web/packages/dag/src/components/form/js-node-config-editor/`
- `tapdata-web/packages/dag/src/nodes/JavaScript.js`
- `tapdata-web/packages/dag/src/nodes/JsProcessor.js`
- `tapdata-web/packages/dag/src/components/form/js-processor/index.tsx`
- `tapdata-web/packages/dag/src/components/NodePanel.vue`

## 自主审核与验证

- `node --test packages/dag/src/components/form/js-node-config-editor/script-params.test.js`：3 个测试通过。
- `pnpm exec prettier --check ...`：本步骤涉及文件格式检查通过。
- `pnpm exec eslint ...`：新增组件无 lint 错误；现有 `NodePanel.vue` 的 prop 修改、JS 节点无用构造函数和 JS 处理器既有规则问题仍存在。
- `pnpm exec vue-tsc -p packages/dag/tsconfig.json --noEmit`：新增组件及其引用没有新增类型错误；仓库整体仍有既有跨包类型错误，详见命令输出。

## 边界说明

- 编辑器只负责通用参数配置，不预置或识别 FTP 专用字段。
- 参数 key 的格式、保留前缀、数量和大小限制由后端 `JsNodeConfigValidator` 最终校验。
- 前端不会读取或展示后端的全部配置，也不会在日志中输出参数值。
