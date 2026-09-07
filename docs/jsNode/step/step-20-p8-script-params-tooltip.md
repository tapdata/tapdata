# 开发步骤 20：脚本参数提示改为标题角标

## 实现目标

移除 JS 节点“脚本参数”区域中直接展示的说明文字，将说明放到“脚本参数”标题旁的 `!` 角标上，用户悬停角标时通过 Formily 的标题提示展示说明内容。JavaScript 节点和迁移 JavaScript 节点保持相同交互。

## 代码变更

- `tapdata-web/packages/dag/src/components/form/js-node-config-editor/index.tsx`
  - 删除编辑器内容区域内直接渲染 `packages_form_js_node_config_hint` 的提示块。
  - 新增 `JsNodeConfigFormItem`，复用“脚本”标题的 `ElTooltip + VIcon(info)` 标题渲染结构。
- `tapdata-web/packages/dag/src/nodes/JavaScript.js`
- `tapdata-web/packages/dag/src/nodes/JsProcessor.js`
  - 将 `scriptParams` 的装饰器替换为 `JsNodeConfigFormItem`，配置提示内容。
- `tapdata-web/packages/dag/src/components/form/js-node-config-editor/style.scss`
  - 删除编辑器内提示块的样式，标题提示样式由复用的“脚本”标题结构提供。

## 自审与验收

- 直接提示块已删除，提示文案仍只作为标题 tooltip 内容存在。
- JavaScript 与迁移 JavaScript 两个节点的 `scriptParams` 均配置了相同的 tooltip。
- 角标直接复用“脚本”标题的 `VIcon(info)` 样式和 Formily `FormItem.BaseItem` 布局，不改变其他标题提示。
- `node --test packages/dag/src/components/form/js-node-config-editor/script-params.test.js`：5 项通过。
- `pnpm exec prettier --check`：4 个变更文件通过。
- 目标 ESLint 检查仍受到原有 `JavaScript.js` 和 `JsProcessor.js` 构造函数 `no-useless-constructor` 报错影响，本步骤未引入新的 lint 报错。
