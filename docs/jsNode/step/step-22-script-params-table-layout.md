# 开发步骤 22：脚本参数改为表格布局

## 实现目标

将“脚本参数”配置行改为表格展示，把参数名、类型、参数值、加密、说明和操作放到表头，输入控件只放在数据行，避免“加密”等标题与控件挤在同一行。

## 代码变更

- `tapdata-web/packages/dag/src/components/form/js-node-config-editor/index.tsx`
  - 使用原生表格的 `thead/tbody` 展示配置列标题和参数行。
  - 保留重复参数名校验、JSON 多行输入、加密值密码框、开关和删除操作。
  - 移除输入框中的重复 placeholder，减少数据行拥挤。
- `tapdata-web/packages/dag/src/components/form/js-node-config-editor/style.scss`
  - 增加表格边框、表头背景、列宽和单元格对齐样式。
  - 加密开关和删除按钮分别在对应单元格内居中。
- `tapdata-web/packages/form/src/locale/lang/en.js`
- `tapdata-web/packages/form/src/locale/lang/zh-CN.js`
- `tapdata-web/packages/form/src/locale/lang/zh-TW.js`
  - 增加“类型”和“操作”表头的多语言文案。

## 自审与验收

- 标题与输入控件位于不同表格行，数据行不再重复显示“加密”等字段标题。
- 表格列宽固定，值和说明列占用主要空间，适配 JSON 多行值编辑。
- `node --test packages/dag/src/components/form/js-node-config-editor/script-params.test.js`：5 项通过。
- `pnpm exec prettier --check`：变更文件通过。
- `eslint`：编辑器 TSX 文件通过。
- `check-i18n`：通过。

