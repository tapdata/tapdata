# 开发步骤 21：移除脚本参数标题的重复悬浮提示

## 问题

自定义 `JsNodeConfigFormItem` 接收 schema 的 `title` 属性后，如果将其继续透传给 `FormItem.BaseItem`，浏览器会把它渲染为原生 `title` 属性。鼠标停留在“脚本参数”附近时，原生 title 提示会与“脚本”样式的 `ElTooltip` 叠加。

## 实现

在 `JsNodeConfigFormItem` 组装 `FormItem.BaseItem` 参数时移除 `title` 和 `label`，仅使用自定义标题节点渲染“脚本参数”；同时继续移除 `tooltip` 和 `tooltipLayout`，避免重复创建 Formily 默认提示。

## 验收

- “脚本参数”只保留一套与“脚本”标题一致的 `ElTooltip + VIcon(info)` 提示。
- 不再向标题根节点透传原生 `title` 属性，避免额外浏览器悬浮提示。
- 脚本参数单测 5 项通过，变更文件通过 Prettier 检查。

